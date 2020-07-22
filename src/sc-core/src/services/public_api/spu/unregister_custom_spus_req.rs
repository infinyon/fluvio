//!
//! # Delete Custom Spus Request
//!
//! Lookup custom-spu in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::FlvErrorCode;
use sc_api::FlvStatus;
use sc_api::spu::*;

use crate::stores::spu::*;
use crate::core::*;

/// Handler for delete custom spu request
pub async fn handle_un_register_custom_spu_request(
    key: CustomSpuKey,
    ctx: SharedContext,
) -> Result<FlvStatus, Error> {
    let spu_store = ctx.spus().store();
    let status = match key {
        CustomSpuKey::Name(spu_name) => {
            debug!("api request: delete custom-spu with name '{}'", spu_name);

            // spu-name must exist
            if let Some(spu) = spu_store.spu(&spu_name).await {
                un_register_custom_spu(&ctx, spu).await
            } else {
                // spu does not exist
                FlvStatus::new(
                    spu_name.clone(),
                    FlvErrorCode::SpuNotFound,
                    Some("not found".to_owned()),
                )
            }
        }
        CustomSpuKey::Id(spu_id) => {
            debug!("api request: delete custom-spu with id '{}'", spu_id);

            // spu-id must exist
            if let Some(spu) = spu_store.get_by_id(spu_id).await {
                un_register_custom_spu(&ctx, spu).await
            } else {
                // spu does not exist
                FlvStatus::new(
                    format!("spu-{}", spu_id),
                    FlvErrorCode::SpuNotFound,
                    Some("not found".to_owned()),
                )
            }
        }
    };

    trace!("flv delete custom-spus resp {:#?}", status);

    Ok(status)
}

/// Generate for delete custom spu operation and return result.
async fn un_register_custom_spu(ctx: &Context, spu: SpuAdminMd) -> FlvStatus {
    let spu_name = spu.name().to_owned();

    // must be Custom Spu
    if !spu.is_custom() {
        return FlvStatus::new(
            spu_name,
            FlvErrorCode::SpuError,
            Some("expected 'Custom' spu, found 'Managed' spu".to_owned()),
        );
    }

    // delete custom spec and return result
    if let Err(err) = ctx.spus().delete(spu_name.clone()).await {
        FlvStatus::new(
            spu_name,
            FlvErrorCode::SpuError,
            Some(format!("error deleting: {}", err)),
        )
    } else {
        FlvStatus::new_ok(spu_name.clone())
    }
}
