use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::{Result, Ok};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInitInput, SmartModuleInitOutput, SmartModuleInitErrorStatus,
};
use wasmtime::{AsContextMut, TypedFunc};

use crate::instance::SmartModuleInstanceContext;

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
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, INIT_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .or_else(|_| func.typed(store))
                .map(|init_fn| Some(Self(init_fn))),
            None => Ok(None),
        }
    }
}

impl SmartModuleInit {
    /// initialize SmartModule
    pub(crate) fn initialize(
        &mut self,
        input: SmartModuleInitInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<()> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let init_output = self.0.call(&mut *store, slice)?;

        if init_output < 0 {
            let internal_error = SmartModuleInitErrorStatus::try_from(init_output)
                .unwrap_or(SmartModuleInitErrorStatus::UnknownError);

            match internal_error {
                SmartModuleInitErrorStatus::InitError => {
                    let output: SmartModuleInitOutput = ctx.read_output(store)?;
                    Err(output.error.into())
                }
                _ => Err(internal_error.into()),
            }
        } else {
            Ok(())
        }
    }
}
