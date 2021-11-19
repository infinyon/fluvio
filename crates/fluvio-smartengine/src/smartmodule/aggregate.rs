use std::convert::TryFrom;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::TypedFunc;

use crate::smartmodule::{SmartEngine, SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance};
use dataplane::smartmodule::{
    SmartModuleAggregateInput, SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
    SmartModuleExtraParams, SmartModuleAggregateOutput,
};

const AGGREGATE_FN_NAME: &str = "aggregate";
type AggregateFn = TypedFunc<(i32, i32), i32>;

pub struct SmartModuleAggregate {
    base: SmartModuleContext,
    aggregate_fn: AggregateFn,
    accumulator: Vec<u8>,
}

impl SmartModuleAggregate {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        accumulator: Vec<u8>,
        version: i16,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(engine, module, params, version)?;
        let aggregate_fn: AggregateFn = base
            .instance
            .get_typed_func(&mut base.store, AGGREGATE_FN_NAME)?;

        Ok(Self {
            base,
            aggregate_fn,
            accumulator,
        })
    }
}

impl SmartModuleInstance for SmartModuleAggregate {
    #[instrument(skip(self,base),fields(offset = base.base_offset))]
    fn process(&mut self, base: SmartModuleInput) -> Result<SmartModuleOutput> {
        debug!("start aggregration");
        let input = SmartModuleAggregateInput {
            base,
            accumulator: self.accumulator.clone(),
        };
        let slice = self.base.write_input(&input)?;
        let aggregate_output = self.aggregate_fn.call(&mut self.base.store, slice)?;

        debug!(aggregate_output);
        if aggregate_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(aggregate_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleAggregateOutput = self.base.read_output()?;
        self.accumulator = output.accumulator;
        Ok(output.base)
    }
    fn params(&self) -> SmartModuleExtraParams {
        self.base.params.clone()
    }
}
