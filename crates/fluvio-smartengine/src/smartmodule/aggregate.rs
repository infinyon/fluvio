use std::convert::TryFrom;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::{
    WasmSlice,
    smartmodule::{SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance},
    error::Error,
};
use dataplane::smartmodule::{
    SmartModuleAggregateInput, SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
    SmartModuleExtraParams, SmartModuleAggregateOutput,
};

const AGGREGATE_FN_NAME: &str = "aggregate";
type OldAggregateFn = TypedFunc<(i32, i32), i32>;
type AggregateFn = TypedFunc<(i32, i32, u32), i32>;

pub struct SmartModuleAggregate {
    base: SmartModuleContext,
    aggregate_fn: AggregateFnKind,
    accumulator: Vec<u8>,
}
pub enum AggregateFnKind {
    Old(OldAggregateFn),
    New(AggregateFn),
}

impl AggregateFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Old(aggregate_fn) => aggregate_fn.call(store, (slice.0, slice.1)),
            Self::New(aggregate_fn) => aggregate_fn.call(store, slice),
        }
    }
}

impl SmartModuleAggregate {
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        accumulator: Vec<u8>,
        version: i16,
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let aggregate_fn: AggregateFnKind = if let Ok(agg_fn) = base
            .instance
            .get_typed_func(&mut base.store, AGGREGATE_FN_NAME)
            .map_err(|_err| Error::NotNamedExport(AGGREGATE_FN_NAME))
        {
            AggregateFnKind::New(agg_fn)
        } else {
            let agg_fn: OldAggregateFn = base
                .instance
                .get_typed_func(&mut base.store, AGGREGATE_FN_NAME)
                .map_err(|_err| Error::NotNamedExport(AGGREGATE_FN_NAME))?;
            AggregateFnKind::Old(agg_fn)
        };

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
