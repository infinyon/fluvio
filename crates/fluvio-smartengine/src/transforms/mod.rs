mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
mod join;
mod join_stream;

pub(crate) use instance::create_transform;

mod instance {

    use anyhow::{Result};
    use wasmtime::{AsContextMut};

    use crate::{
        instance::{SmartModuleTransform, SmartModuleInstanceContext},
        error::EngineError,
    };

    use super::{
        filter::SmartModuleFilter, map::SmartModuleMap, filter_map::SmartModuleFilterMap,
        array_map::SmartModuleArrayMap, join::SmartModuleJoin, join_stream::SmartModuleJoinStream,
        aggregate::SmartModuleAggregate,
    };

    pub(crate) fn create_transform(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Box<dyn SmartModuleTransform>, EngineError> {
        if let Some(tr) = SmartModuleFilter::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleMap::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleFilterMap::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleArrayMap::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleJoin::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleJoinStream::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else if let Some(tr) = SmartModuleAggregate::try_instantiate(ctx, store)?
            .map(|transform| Box::new(transform) as Box<dyn SmartModuleTransform>)
        {
            Ok(tr)
        } else {
            Err(EngineError::UnknownSmartModule)
        }
    }
}
