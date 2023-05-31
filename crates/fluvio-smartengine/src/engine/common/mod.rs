mod init;
mod transforms;
pub(crate) use init::*;
pub(crate) use transforms::*;
use anyhow::Result;
use fluvio_protocol::{Decoder, Encoder};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInitInput, SmartModuleInput, SmartModuleOutput,
};

pub(crate) trait WasmInstance {
    type Context;
    type Func: WasmFn<Context = Self::Context> + Send + Sync + 'static;

    fn params(&self) -> SmartModuleExtraParams;

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
pub(crate) trait WasmFn {
    type Context;
    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32>;
}

pub(crate) struct SmartModuleInstance<I: WasmInstance<Func = F>, F: WasmFn> {
    pub instance: I,
    pub transform: Box<dyn DowncastableTransform<I>>,
    pub init: Option<SmartModuleInit<F>>,
}

impl<I: WasmInstance<Func = F>, F: WasmFn + Send + Sync> SmartModuleInstance<I, F> {
    #[cfg(test)]
    #[allow(clippy::borrowed_box)]
    pub(crate) fn transform(&self) -> &Box<dyn DowncastableTransform<I>> {
        &self.transform
    }

    #[cfg(test)]
    pub(crate) fn get_init(&self) -> &Option<SmartModuleInit<F>> {
        &self.init
    }

    pub(crate) fn new(
        instance: I,
        init: Option<SmartModuleInit<F>>,
        transform: Box<dyn DowncastableTransform<I>>,
    ) -> Self {
        Self {
            instance,
            init,
            transform,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut I::Context,
    ) -> Result<SmartModuleOutput> {
        self.transform.process(input, &mut self.instance, ctx)
    }

    pub fn init<C>(&mut self, ctx: &mut I::Context) -> Result<()>
    where
        I: WasmInstance<Context = C>,
        F: WasmFn<Context = C>,
    {
        if let Some(init) = &mut self.init {
            let input = SmartModuleInitInput {
                params: self.instance.params(),
            };
            init.initialize(input, &mut self.instance, ctx)
        } else {
            Ok(())
        }
    }
}
