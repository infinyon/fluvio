use std::any::Any;

use anyhow::Result;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
    SmartModuleInitErrorStatus, SmartModuleInitOutput, SmartModuleInitInput,
};

pub trait WasmInstance {
    type Context;
    type Func: WasmFn<Context = Self::Context>;

    fn get_fn(&self, name: &str, ctx: &mut Self::Context) -> Result<Option<Self::Func>>;

    fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        ctx: &mut Self::Context,
    ) -> Result<(i32, i32, i32)>;
    fn read_output<D: Decoder + Default>(&mut self, ctx: &mut Self::Context) -> Result<D>;
}

/// All smartmodule wasm functions have the same ABI:
/// `(ptr: *mut u8, len: usize, version: i16) -> i32`, which is `(param i32 i32 i32) (result i32)` in wasm.
pub trait WasmFn {
    type Context;
    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32>;
}

pub trait SmartModuleTransform<I: WasmInstance>: Send + Sync {
    /// transform records
    fn process(
        &mut self,
        input: SmartModuleInput,
        instance: &mut I,
        ctx: &mut I::Context,
    ) -> Result<SmartModuleOutput>;

    /// return name of transform, this is used for identifying transform and debugging
    fn name(&self) -> &str;
}

// In order turn to any, need following magic trick
pub(crate) trait DowncastableTransform<I: WasmInstance>:
    SmartModuleTransform<I> + Any
{
    fn as_any(&self) -> &dyn Any;
}

impl<T: SmartModuleTransform<I> + Any, I: WasmInstance> DowncastableTransform<I> for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct SimpleTransformImpl<F: WasmFn + Send + Sync> {
    name: String,
    func: F,
}

impl<I: WasmInstance, F: WasmFn<Context = I::Context> + Send + Sync> SmartModuleTransform<I>
    for SimpleTransformImpl<F>
{
    fn process(
        &mut self,
        input: SmartModuleInput,
        instance: &mut I,
        ctx: &mut I::Context,
    ) -> Result<SmartModuleOutput> {
        let (ptr, len, version) = instance.write_input(&input, ctx)?;
        let output = self.func.call(ptr, len, version, ctx)?;

        if output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = instance.read_output(ctx)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl<F: WasmFn + Send + Sync> SimpleTransformImpl<F> {
    pub(crate) fn try_instantiate<I>(
        name: &str,
        instance: &mut I,
        ctx: &mut <I as WasmInstance>::Context,
    ) -> Result<Option<Self>>
    where
        I: WasmInstance<Func = F>,
        F: WasmFn<Context = I::Context>,
    {
        let func = instance
            .get_fn(name, ctx)?
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))?;
        Ok(Some(Self {
            name: name.to_owned(),
            func,
        }))
    }
}

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

mod wasmtime {
    use anyhow::Result;
    use fluvio_protocol::{Encoder, Decoder};
    use tracing::debug;

    use std::sync::Arc;

    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
    use wasmtime::{Instance, Store};

    use crate::engine::wasmtime::instance::RecordsCallBack;

    pub struct WasmTimeInstance {
        instance: Instance,
        records_cb: Arc<RecordsCallBack>,
        params: SmartModuleExtraParams,
        version: i16,
    }
    pub struct WasmTimeContext {
        store: Store<()>,
    }

    pub type WasmTimeFn = wasmtime::TypedFunc<(i32, i32, i32), i32>;

    impl super::WasmInstance for WasmTimeInstance {
        type Context = WasmTimeContext;

        type Func = WasmTimeFn;

        fn get_fn(&self, name: &str, ctx: &mut Self::Context) -> Result<Option<Self::Func>> {
            match self.instance.get_func(&mut ctx.store, name) {
                Some(func) => {
                    // check type signature
                    func.typed(&mut ctx.store)
                        .or_else(|_| func.typed(&ctx.store))
                        .map(|f| Some(f))
                }
                None => Ok(None),
            }
        }

        fn write_input<E: Encoder>(
            &mut self,
            input: &E,
            ctx: &mut Self::Context,
        ) -> anyhow::Result<(i32, i32, i32)> {
            self.records_cb.clear();
            let mut input_data = Vec::new();
            input.encode(&mut input_data, self.version)?;
            debug!(
                len = input_data.len(),
                version = self.version,
                "input encoded"
            );
            let array_ptr = crate::engine::wasmtime::memory::copy_memory_to_instance(
                &mut ctx.store,
                &self.instance,
                &input_data,
            )?;
            let length = input_data.len();
            Ok((array_ptr as i32, length as i32, self.version as i32))
        }

        fn read_output<D: Decoder + Default>(&mut self, ctx: &mut Self::Context) -> Result<D> {
            let bytes = self
                .records_cb
                .get()
                .and_then(|m| m.copy_memory_from(&ctx.store).ok())
                .unwrap_or_default();
            let mut output = D::default();
            output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
            Ok(output)
        }
    }

    impl super::WasmFn for WasmTimeFn {
        type Context = WasmTimeContext;

        fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32> {
            WasmTimeFn::call(self, &mut ctx.store, (ptr, len, version))
        }
    }
}
