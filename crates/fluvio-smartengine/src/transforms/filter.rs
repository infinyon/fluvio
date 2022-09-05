use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::{
    WasmSlice,
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const FILTER_FN_NAME: &str = "filter";
type BaseFilterFn = TypedFunc<(i32, i32), i32>;
type FilterFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub(crate) struct SmartModuleFilter {
    filter_fn: FilterFnKind,
}

enum FilterFnKind {
    Base(BaseFilterFn),
    Param(FilterFnWithParam),
}

impl Debug for FilterFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_) => write!(f, "BaseFilterFn"),
            Self::Param(_) => write!(f, "FilterFnWithParam"),
        }
    }
}

impl FilterFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(filter_fn) => filter_fn.call(store, (slice.0, slice.1)),
            Self::Param(filter_fn) => filter_fn.call(store, slice),
        }
    }
}

impl SmartModuleFilter {
    /// Try to create filter by matching function, if function is not found, then return empty
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        match ctx.get_wasm_func(store, FILTER_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .map(FilterFnKind::Base)
                .or_else(|_| func.typed(store).map(FilterFnKind::Param))
                .map(|filter_fn| Some(Self { filter_fn }))
                .map_err(|wasm_err| EngineError::TypeConversion(FILTER_FN_NAME, wasm_err)),
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleFilter {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let filter_output = self.filter_fn.call(&mut *store, slice)?;

        if filter_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(filter_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        FILTER_FN_NAME
    }
}
