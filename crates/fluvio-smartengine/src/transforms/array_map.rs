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

pub(crate) const ARRAY_MAP_FN_NAME: &str = "array_map";
type BaseArrayMapFn = TypedFunc<(i32, i32), i32>;
type ArrayMapFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub(crate) struct SmartModuleArrayMap {
    array_map_fn: ArrayMapFnKind,
}

enum ArrayMapFnKind {
    Base(BaseArrayMapFn),
    Param(ArrayMapFnWithParam),
}

impl Debug for ArrayMapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(..) => write!(f, "BaseArrayMapFn"),
            Self::Param(..) => write!(f, "ArrayMapFnWithParam"),
        }
    }
}

impl ArrayMapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(array_map_fn) => array_map_fn.call(store, (slice.0, slice.1)),
            Self::Param(array_map_fn) => array_map_fn.call(store, slice),
        }
    }
}

impl SmartModuleArrayMap {
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        match ctx.get_wasm_func(&mut *store, ARRAY_MAP_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .map(ArrayMapFnKind::Base)
                    .or_else(|_| func.typed(store).map(ArrayMapFnKind::Param))
                    .map(|array_map_fn| Some(Self { array_map_fn }))
                    .map_err(|wasm_err| EngineError::TypeConversion(ARRAY_MAP_FN_NAME, wasm_err))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleArrayMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let map_output = self.array_map_fn.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        ARRAY_MAP_FN_NAME
    }
}
