use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInitErrorStatus, SmartModuleInitInput, SmartModuleInitOutput,
};

use super::{WasmFn, WasmInstance};

pub(crate) const INIT_FN_NAME: &str = "init";

pub(crate) struct SmartModuleInit<F: WasmFn>(F);

impl<F: WasmFn> std::fmt::Debug for SmartModuleInit<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InitFn")
    }
}

impl<F: WasmFn + Send + Sync> SmartModuleInit<F> {
    pub(crate) fn try_instantiate<I>(
        instance: &mut I,
        ctx: &mut <I as WasmInstance>::Context,
    ) -> Result<Option<Self>>
    where
        I: WasmInstance<Func = F>,
        F: WasmFn<Context = I::Context>,
    {
        match instance.get_fn(INIT_FN_NAME, ctx)? {
            Some(func) => Ok(Some(Self(func))),
            None => Ok(None),
        }
    }
}

impl<F: WasmFn + Send + Sync> SmartModuleInit<F> {
    /// initialize SmartModule
    pub(crate) fn initialize<I>(
        &mut self,
        input: SmartModuleInitInput,
        instance: &mut I,
        ctx: &mut I::Context,
    ) -> Result<()>
    where
        I: WasmInstance,
        F: WasmFn<Context = I::Context>,
    {
        let (ptr, len, version) = instance.write_input(&input, ctx)?;
        let init_output = self.0.call(ptr, len, version, ctx)?;

        if init_output < 0 {
            let internal_error = SmartModuleInitErrorStatus::try_from(init_output)
                .unwrap_or(SmartModuleInitErrorStatus::UnknownError);

            match internal_error {
                SmartModuleInitErrorStatus::InitError => {
                    let output: SmartModuleInitOutput = instance.read_output(ctx)?;
                    Err(output.error.into())
                }
                _ => Err(internal_error.into()),
            }
        } else {
            Ok(())
        }
    }
}
