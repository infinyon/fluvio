use tracing::debug;
use tracing::trace;

use std::io::Error;

use sc_api::FlvStatus;

use crate::core::*;

/// Handler for delete spu group request
pub async fn handle_delete_spu_group(name: String, ctx: SharedContext) -> Result<FlvStatus, Error> {
    use kf_protocol::api::FlvErrorCode;

    debug!("delete spg group: {}", name);

    let status = if let Some(_) = ctx.spgs().store().value(&name).await {
        if let Err(err) = ctx.spgs().delete(name.clone()).await {
            FlvStatus::new(name.clone(), FlvErrorCode::SpuError, Some(err.to_string()))
        } else {
            FlvStatus::new_ok(name)
        }
    } else {
        FlvStatus::new(
            name,
            FlvErrorCode::SpuNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete spu group resp {:#?}", status);

    Ok(status)
}
