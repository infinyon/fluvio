mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
mod join;
mod join_stream;

mod instance {

    use anyhow::{Result};
    use wasmtime::{Module};

    use crate::{
        instance::{SmartModuleInstance, SmartModuleTransform, SmartModuleInstanceContext},
        SmartModuleChain,
        error::Error,
    };

    use super::{filter::SmartModuleFilter, map::SmartModuleMap};

    pub(crate) fn create_transform(
        ctx: SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<Box<dyn SmartModuleTransform>, Error> {
        SmartModuleFilter::try_instantiate(ctx, chain)?
            .map(|transform| Ok(Box::new(transform) as Box<dyn SmartModuleTransform>))
            .unwrap_or_else(|| Err(Error::UnknownSmartModule))
    }
}
