use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use wasmtime::{AsContextMut, TypedFunc};

use crate::{
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const FILTER_MAP_FN_NAME: &str = "filter_map";

type WasmFilterMapFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleFilterMap(WasmFilterMapFn);

impl Debug for SmartModuleFilterMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilterMapFnWithParam")
    }
}

impl SmartModuleFilterMap {
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        match ctx.get_wasm_func(&mut *store, FILTER_MAP_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .or_else(|_| func.typed(store))
                    .map(|filter_map_fn| Some(Self(filter_map_fn)))
                    .map_err(|wasm_err| EngineError::TypeConversion(FILTER_MAP_FN_NAME, wasm_err))
            }
            None => Ok(None),
        }
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
        let map_output = self.0.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        FILTER_MAP_FN_NAME
    }
}
