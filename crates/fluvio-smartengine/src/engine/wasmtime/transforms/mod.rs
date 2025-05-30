mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
pub(crate) use instance::create_transform;
mod simple_transform;

mod instance {

    use anyhow::Result;
    use wasmtime::AsContextMut;

    use crate::engine::{error::EngineError, SmartModuleInitialData};
    use super::super::instance::{SmartModuleInstanceContext, DowncastableTransform};

    use super::{
        simple_transform::{
            SimpleTansform, FILTER_FN_NAME, MAP_FN_NAME, FILTER_MAP_FN_NAME, ARRAY_MAP_FN_NAME,
        },
        aggregate::SmartModuleAggregate,
    };

    pub(crate) fn create_transform(
        ctx: &SmartModuleInstanceContext,
        initial_data: SmartModuleInitialData,
        store: &mut impl AsContextMut,
    ) -> Result<Box<dyn DowncastableTransform>> {
        if let Some(tr) = SimpleTansform::try_instantiate(FILTER_FN_NAME, ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SimpleTansform::try_instantiate(MAP_FN_NAME, ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SimpleTansform::try_instantiate(FILTER_MAP_FN_NAME, ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SimpleTansform::try_instantiate(ARRAY_MAP_FN_NAME, ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleAggregate::try_instantiate(ctx, initial_data, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else {
            Err(EngineError::UnknownSmartModule.into())
        }
    }
}
