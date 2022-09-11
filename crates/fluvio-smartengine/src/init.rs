use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError, SmartModuleInitInput,
    SmartModuleInitOutput,
};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::{
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const INIT_FN_NAME: &str = "init";
type WasmInitFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleInit(WasmInitFn);

impl Debug for SmartModuleInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InitFn")
    }
}

impl SmartModuleInit {
    /// Try to create filter by matching function, if function is not found, then return empty
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        match ctx.get_wasm_func(store, INIT_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .or_else(|_| func.typed(store))
                .map(|init_fn| Some(Self(init_fn)))
                .map_err(|wasm_err| EngineError::TypeConversion(INIT_FN_NAME, wasm_err)),
            None => Ok(None),
        }
    }
}

impl SmartModuleInit {
    pub(crate) fn process(
        &mut self,
        input: SmartModuleInitInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleInitOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let init_output = self.0.call(&mut *store, slice)?;

        if init_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(init_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleInitOutput = ctx.read_output(store)?;
        Ok(output)
    }
}
