use std::any::Any;

use anyhow::Result;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
    SmartModuleInitErrorStatus, SmartModuleInitOutput, SmartModuleInitInput,
    SmartModuleExtraParams,
};

use crate::SmartModuleInitialData;

use super::error::EngineError;

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

pub(crate) trait SmartModuleTransform<I: WasmInstance>: Send + Sync {
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

pub(crate) struct SimpleTransform<F: WasmFn + Send + Sync> {
    name: String,
    func: F,
}

impl<I: WasmInstance, F: WasmFn<Context = I::Context> + Send + Sync> SmartModuleTransform<I>
    for SimpleTransform<F>
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

impl<F: WasmFn + Send + Sync> SimpleTransform<F> {
    pub(crate) fn try_instantiate<I, C>(
        name: &str,
        instance: &mut I,
        ctx: &mut C,
    ) -> Result<Option<Self>>
    where
        I: WasmInstance<Func = F, Context = C>,
        F: WasmFn<Context = C>,
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

pub(crate) const FILTER_FN_NAME: &str = "filter";
pub(crate) const MAP_FN_NAME: &str = "map";
pub(crate) const FILTER_MAP_FN_NAME: &str = "filter_map";
pub(crate) const ARRAY_MAP_FN_NAME: &str = "array_map";

pub(crate) fn create_transform<I, C>(
    instance: &mut I,
    ctx: &mut C,
    _initial_data: SmartModuleInitialData,
) -> Result<Box<dyn DowncastableTransform<I>>>
where
    I: WasmInstance<Context = C>,
{
    if let Some(tr) = SimpleTransform::try_instantiate(FILTER_FN_NAME, instance, ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
    {
        Ok(tr)
    } else if let Some(tr) = SimpleTransform::try_instantiate(MAP_FN_NAME, instance, ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
    {
        Ok(tr)
    } else if let Some(tr) = SimpleTransform::try_instantiate(FILTER_MAP_FN_NAME, instance, ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
    {
        Ok(tr)
    } else if let Some(tr) = SimpleTransform::try_instantiate(ARRAY_MAP_FN_NAME, instance, ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
    {
        Ok(tr)
        // TODO: AGGREGATE
        // } else if let Some(tr) = SmartModuleAggregate::try_instantiate(ctx, initial_data, store)?
        //     .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
        // {
        //     Ok(tr)
    } else {
        Err(EngineError::UnknownSmartModule.into())
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
