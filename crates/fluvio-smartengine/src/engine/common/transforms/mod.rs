use std::any::Any;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleAggregateInput, SmartModuleAggregateOutput, SmartModuleInput, SmartModuleOutput,
    SmartModuleTransformErrorStatus,
};
use tracing::debug;

use crate::{engine::error::EngineError, SmartModuleInitialData};
use super::{WasmFn, WasmInstance};

mod filter;
mod map;
mod filter_map;
mod array_map;
mod aggregate;

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
        match instance.get_fn(name, ctx)? {
            Some(func) => Ok(Some(Self {
                name: name.to_owned(),
                func,
            })),
            None => Ok(None),
        }
    }
}

pub(crate) const FILTER_FN_NAME: &str = "filter";
pub(crate) const MAP_FN_NAME: &str = "map";
pub(crate) const FILTER_MAP_FN_NAME: &str = "filter_map";
pub(crate) const ARRAY_MAP_FN_NAME: &str = "array_map";
pub(crate) const AGGREGATE_FN_NAME: &str = "aggregate";

pub(crate) struct AggregateTransform<F: WasmFn + Send + Sync> {
    func: F,
    accumulator: Vec<u8>,
}

impl<F: WasmFn + Send + Sync> AggregateTransform<F> {
    #[cfg(test)]
    pub(crate) fn accumulator(&self) -> &[u8] {
        &self.accumulator
    }

    pub(crate) fn try_instantiate<I, C>(
        instance: &mut I,
        ctx: &mut C,
        initial_data: SmartModuleInitialData,
    ) -> Result<Option<Self>>
    where
        I: WasmInstance<Func = F, Context = C>,
        F: WasmFn<Context = C>,
    {
        // get initial data
        let accumulator = match initial_data {
            SmartModuleInitialData::Aggregate { accumulator } => accumulator,
            SmartModuleInitialData::None => {
                // if no initial data, then we initialize as default
                vec![]
            }
        };

        match instance.get_fn(AGGREGATE_FN_NAME, ctx)? {
            Some(func) => Ok(Some(Self { func, accumulator })),
            None => Ok(None),
        }
    }
}

impl<I: WasmInstance, F: WasmFn<Context = I::Context> + Send + Sync> SmartModuleTransform<I>
    for AggregateTransform<F>
{
    fn process(
        &mut self,
        input: SmartModuleInput,
        instance: &mut I,
        ctx: &mut I::Context,
    ) -> Result<SmartModuleOutput> {
        debug!("start aggregration");
        let input = SmartModuleAggregateInput {
            base: input,
            accumulator: self.accumulator.clone(),
        };

        let (ptr, len, version) = instance.write_input(&input, ctx)?;
        let aggregate_output = self.func.call(ptr, len, version, ctx)?;

        if aggregate_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(aggregate_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleAggregateOutput = instance.read_output(ctx)?;
        self.accumulator = output.accumulator;
        Ok(output.base)
    }

    fn name(&self) -> &str {
        AGGREGATE_FN_NAME
    }
}

pub(crate) fn create_transform<I, C>(
    instance: &mut I,
    ctx: &mut C,
    initial_data: SmartModuleInitialData,
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
    } else if let Some(tr) = AggregateTransform::try_instantiate(instance, ctx, initial_data)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<I>>)
    {
        Ok(tr)
    } else {
        Err(EngineError::UnknownSmartModule.into())
    }
}
