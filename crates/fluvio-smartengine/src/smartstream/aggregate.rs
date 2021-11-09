use std::convert::TryFrom;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::TypedFunc;

use crate::smartstream::{SmartEngine, SmartStreamModule, SmartStreamContext, SmartStream};
use dataplane::smartstream::{
    SmartStreamAggregateInput, SmartStreamInput, SmartStreamOutput, SmartStreamInternalError,
    SmartStreamExtraParams, SmartStreamAggregateOutput
};

const AGGREGATE_FN_NAME: &str = "aggregate";
type AggregateFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamAggregate {
    base: SmartStreamContext,
    aggregate_fn: AggregateFn,
    accumulator: Vec<u8>,
}

impl SmartStreamAggregate {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
        accumulator: Vec<u8>,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
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

impl SmartStream for SmartStreamAggregate {
    #[instrument(skip(self,base),fields(offset = base.base_offset))]
    fn process(&mut self, base: SmartStreamInput) -> Result<SmartStreamOutput> {
        debug!("start aggregration");
        let input = SmartStreamAggregateInput {
            base,
            accumulator: self.accumulator.clone(),
        };
        let slice = self.base.write_input(&input)?;
        println!("-----------aggregate_fn call");
        let aggregate_output = self.aggregate_fn.call(&mut self.base.store, slice)?;

        debug!(aggregate_output);
        if aggregate_output < 0 {
            let internal_error = SmartStreamInternalError::try_from(aggregate_output)
                .unwrap_or(SmartStreamInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartStreamAggregateOutput = self.base.read_output()?;

        self.accumulator = output.accumulator;

        Ok(output.base)
    }
    fn params(&self) -> SmartStreamExtraParams {
        self.base.params.clone()
    }
}
