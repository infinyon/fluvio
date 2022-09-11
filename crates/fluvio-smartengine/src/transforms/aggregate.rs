use std::convert::TryFrom;
use std::fmt::Debug;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::{AsContextMut, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleAggregateInput, SmartModuleInternalError,
    SmartModuleAggregateOutput,
};
use crate::{
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState, SmartModuleInitialData,
};

pub(crate) const AGGREGATE_FN_NAME: &str = "aggregate";

type WasmAggregateFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleAggregate {
    aggregate_fn: WasmAggregateFn,
    accumulator: Vec<u8>,
}

impl Debug for SmartModuleAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AggregateFn")
    }
}

impl SmartModuleAggregate {
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        initial_data: SmartModuleInitialData,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        // get initial -data
        let accumulator = match initial_data {
            SmartModuleInitialData::Aggregate { accumulator } => accumulator,
            SmartModuleInitialData::None => {
                // if no initial data, then we initialize as default
                vec![]
            }
        };

        match ctx.get_wasm_func(&mut *store, AGGREGATE_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .or_else(|_| func.typed(store))
                    .map(|aggregate_fn| {
                        Some(Self {
                            aggregate_fn,
                            accumulator,
                        })
                    })
                    .map_err(|wasm_err| EngineError::TypeConversion(AGGREGATE_FN_NAME, wasm_err))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleAggregate {
    #[instrument(skip(self,ctx,store),fields(offset = input.base_offset))]
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        debug!("start aggregration");
        let input = SmartModuleAggregateInput {
            base: input,
            accumulator: self.accumulator.clone(),
        };
        let slice = ctx.write_input(&input, &mut *store)?;
        let aggregate_output = self.aggregate_fn.call(&mut *store, slice)?;

        debug!(aggregate_output);
        if aggregate_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(aggregate_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleAggregateOutput = ctx.read_output(store)?;
        self.accumulator = output.accumulator;
        Ok(output.base)
    }

    fn name(&self) -> &str {
        AGGREGATE_FN_NAME
    }
}
