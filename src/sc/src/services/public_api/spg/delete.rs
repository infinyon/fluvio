use tracing::debug;
use tracing::trace;

use std::io::Error;

use fluvio_sc_schema::Status;

use crate::core::*;

/// Handler for delete spu group request
pub async fn handle_delete_spu_group(name: String, ctx: SharedContext) -> Result<Status, Error> {
    use dataplane_protocol::ErrorCode;

    debug!("delete spg group: {}", name);

    let status = if ctx.spgs().store().value(&name).await.is_some() {
        if let Err(err) = ctx.spgs().delete(name.clone()).await {
            Status::new(name.clone(), ErrorCode::SpuError, Some(err.to_string()))
        } else {
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name,
            ErrorCode::SpuNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete spu group resp {:#?}", status);

    Ok(status)
}
