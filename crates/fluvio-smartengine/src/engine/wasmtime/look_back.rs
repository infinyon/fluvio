use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::{Result, Ok};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleLookbackOutput, SmartModuleLookbackErrorStatus, SmartModuleInput,
};
use wasmtime::{AsContextMut, TypedFunc};

use super::instance::SmartModuleInstanceContext;

const LOOKBACK_FN_NAME: &str = "look_back";
type LookBackFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleLookBack(LookBackFn);

impl Debug for SmartModuleLookBack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LookBackFn")
    }
}

impl SmartModuleLookBack {
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, LOOKBACK_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .or_else(|_| func.typed(store))
                .map(Self)
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn call(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<()> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let output = self.0.call(&mut *store, slice)?;

        if output < 0 {
            let internal_error = SmartModuleLookbackErrorStatus::try_from(output)
                .unwrap_or(SmartModuleLookbackErrorStatus::UnknownError);

            match internal_error {
                SmartModuleLookbackErrorStatus::PropagatedError => {
                    let output: SmartModuleLookbackOutput = ctx.read_output(store)?;
                    Err(output.error.into())
                }
                _ => Err(internal_error.into()),
            }
        } else {
            Ok(())
        }
    }
}
