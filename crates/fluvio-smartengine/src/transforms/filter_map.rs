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

const FILTER_MAP_FN_NAME: &str = "filter_map";
type BaseFilterMapFn = TypedFunc<(i32, i32), i32>;
type FilterMapFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub(crate) struct SmartModuleFilterMap {
    filter_map_fn: FilterMapFnKind,
}

enum FilterMapFnKind {
    Base(BaseFilterMapFn),
    Param(FilterMapFnWithParam),
}

impl Debug for FilterMapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_) => write!(f, "BaseFilterMapFn"),
            Self::Param(_) => write!(f, "FilterMapFnWithParam"),
        }
    }
}

impl FilterMapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(filter_fn) => filter_fn.call(store, (slice.0, slice.1)),
            Self::Param(filter_fn) => filter_fn.call(store, slice),
        }
    }
}

impl SmartModuleFilterMap {
    pub fn try_instantiate(
        base: SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        base.get_wasm_func(&mut *store, FILTER_MAP_FN_NAME)
            .ok_or(EngineError::NotNamedExport(FILTER_MAP_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed(&mut *store)
                    .map(|typed_fn| FilterMapFnKind::Base(typed_fn))
                    .or_else(|_| {
                        func.typed(store)
                            .map(|typed_fn| FilterMapFnKind::Param(typed_fn))
                    })
                    .map(|filter_map_fn| Some(Self { filter_map_fn }))
                    .map_err(|wasm_err| EngineError::TypeConversion(FILTER_MAP_FN_NAME, wasm_err))
            })
    }
}

impl SmartModuleTransform for SmartModuleFilterMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let map_output = self.filter_map_fn.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }
}
