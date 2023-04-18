mod filter;

use crate::engine::{
    error::EngineError,
    SmartModuleInitialData,
    common::{DowncastableTransform, SimpleTransformImpl},
};

use super::{WasmedgeInstance, WasmedgeContext};
use anyhow::Result;

pub(crate) fn create_transform(
    instance: &mut WasmedgeInstance,
    ctx: &mut WasmedgeContext,
    _initial_data: SmartModuleInitialData,
) -> Result<Box<dyn DowncastableTransform<WasmedgeInstance>>> {
    if let Some(tr) = SimpleTransformImpl::try_instantiate("filter", instance, ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform<WasmedgeInstance>>)
    {
        Ok(tr)
    } else {
        Err(EngineError::UnknownSmartModule.into())
    }
}
