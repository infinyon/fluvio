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

    use super::{filter::SmartModuleFilter, map::SmartModuleMap};

    pub(crate) fn create_transform(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Box<dyn SmartModuleTransform>, EngineError> {
        SmartModuleFilter::try_instantiate(ctx, store)?
            .map(|transform| Ok(Box::new(transform) as Box<dyn SmartModuleTransform>))
            .unwrap_or_else(|| Err(EngineError::UnknownSmartModule))
    }
}
