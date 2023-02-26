mod filter;

use crate::{error::EngineError, SmartModuleInitialData};

use self::filter::SmartModuleFilter;
use super::instance::{DowncastableTransform, SmartModuleInstanceContext};
use anyhow::Result;

pub(crate) fn create_transform(
    ctx: &SmartModuleInstanceContext,
    initial_data: SmartModuleInitialData,
    // store: &mut impl AsContextMut,
) -> Result<Box<dyn DowncastableTransform>> {
    if let Some(tr) = SmartModuleFilter::try_instantiate(ctx)?
        .map(|transform| Box::new(transform) as Box<dyn DowncastableTransform>)
    {
        Ok(tr)
    } else {
        Err(EngineError::UnknownSmartModule.into())
    }
}
