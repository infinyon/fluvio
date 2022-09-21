use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
};
use crate::{
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const MAP_FN_NAME: &str = "map";

type WasmMapFn = TypedFunc<(i32, i32, u32), i32>;

pub struct SmartModuleMap(WasmMapFn);

impl Debug for SmartModuleMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MapFnWithParam")
    }
}

impl SmartModuleMap {
    #[tracing::instrument(skip(ctx, store))]
    pub(crate) fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        match ctx.get_wasm_func(store, MAP_FN_NAME) {
            Some(func) => {
                // check type signature
                func.typed(&mut *store)
                    .or_else(|_| func.typed(&mut *store))
                    .map(|map_fn| Some(Self(map_fn)))
                    .map_err(|wasm_err| EngineError::TypeConversion(MAP_FN_NAME, wasm_err))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let map_output = self.0.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(map_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        MAP_FN_NAME
    }
}
