use std::convert::TryFrom;
use std::fmt::Debug;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleAggregateInput, SmartModuleInternalError,
    SmartModuleAggregateOutput,
};
use crate::{
    WasmSlice,
    error::Error,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    SmartModuleChain,
};

const AGGREGATE_FN_NAME: &str = "aggregate";
type BaseAggregateFn = TypedFunc<(i32, i32), i32>;
type AggregateFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleAggregate {
    aggregate_fn: AggregateFnKind,
    accumulator: Vec<u8>,
}
pub enum AggregateFnKind {
    Base(BaseAggregateFn),
    Param(AggregateFnWithParam),
}

impl Debug for AggregateFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_aggregate_fn) => write!(f, "OldAggregateFn"),
            Self::Param(_aggregate_fn) => write!(f, "AggregateFn"),
        }
    }
}

impl AggregateFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(aggregate_fn) => aggregate_fn.call(store, (slice.0, slice.1)),
            Self::Param(aggregate_fn) => aggregate_fn.call(store, slice),
        }
    }
}

impl SmartModuleAggregate {
    pub fn try_instantiate(
        base: SmartModuleInstanceContext,
        chain: &SmartModuleChain,
    ) -> Result<Option<Self>, Error> {
        base.get_wasm_func(chain, AGGREGATE_FN_NAME)
            .ok_or(Error::NotNamedExport(AGGREGATE_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed()
                    .map(|typed_fn| AggregateFnKind::Base(typed_fn))
                    .or_else(|_| {
                        func.typed()
                            .map(|typed_fn| AggregateFnKind::Param(typed_fn))
                    })
                    .map(|aggregate_fn| {
                        Some(Self {
                            aggregate_fn,
                            accumulator: vec![],
                        })
                    })
                    .map_err(|wasm_err| Error::TypeConversion(AGGREGATE_FN_NAME, wasm_err))
            })
    }
}

/*
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
    {
        AggregateFnKind::New(agg_fn)
    } else {
        let agg_fn: OldAggregateFn = base
            .instance
            .get_typed_func(&mut base.store, AGGREGATE_FN_NAME)
            .map_err(|err| Error::NotNamedExport(AGGREGATE_FN_NAME, err))?;
        AggregateFnKind::Old(agg_fn)
    };

    Ok(Self {
        base,
        aggregate_fn,
        accumulator,
    })
}
*/

impl SmartModuleTransform for SmartModuleAggregate {
    #[instrument(skip(self,base),fields(offset = base.base_offset))]
    fn process(
        &mut self,
        base: SmartModuleInput,
        ctx: &SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput> {
        debug!("start aggregration");
        let input = SmartModuleAggregateInput {
            base,
            accumulator: self.accumulator.clone(),
        };
        let slice = ctx.write_input(&input, chain)?;
        let aggregate_output = self.aggregate_fn.call(chain.as_context_mut(), slice)?;

        debug!(aggregate_output);
        if aggregate_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(aggregate_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleAggregateOutput = ctx.base.read_output(chain)?;
        self.accumulator = output.accumulator;
        Ok(output.base)
    }
}
