pub(crate) mod filter;
pub(crate) mod map;
pub(crate) mod array_map;
pub(crate) mod filter_map;
pub(crate) mod aggregate;

pub(crate) use instance::create_transform;

mod instance {

    use anyhow::{Result};
    use wasmtime::AsContextMut;

    use crate::{
        instance::{SmartModuleInstanceContext, DowncastableTransform},
        error::EngineError,
        SmartModuleInitialData,
    };

    use super::{
        filter::SmartModuleFilter, map::SmartModuleMap, filter_map::SmartModuleFilterMap,
        array_map::SmartModuleArrayMap, aggregate::SmartModuleAggregate,
    };

    pub(crate) fn create_transform(
        ctx: &SmartModuleInstanceContext,
        initial_data: SmartModuleInitialData,
        store: &mut impl AsContextMut,
    ) -> Result<Box<dyn DowncastableTransform>> {
        if let Some(tr) = SmartModuleFilter::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleMap::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleFilterMap::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleArrayMap::try_instantiate(ctx, store)?
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
