//!
//! # Delete Custom Spus Request
//!
//! Lookup custom-spu in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use kf_protocol::api::FlvErrorCode;
use sc_api::{FlvResponseMessage};
use sc_api::spu::{FlvDeleteCustomSpusRequest, FlvDeleteCustomSpusResponse};
use sc_api::spu::FlvCustomSpu;
use k8_metadata::spu::SpuSpec as K8SpuSpec;
use k8_metadata_client::MetadataClient;

use crate::core::spus::SpuKV;
use super::PublicContext;

/// Handler for delete custom spu request
pub async fn handle_delete_custom_spus_request<C>(
    request: RequestMessage<FlvDeleteCustomSpusRequest>,
    ctx: &PublicContext<C>,
) -> Result<ResponseMessage<FlvDeleteCustomSpusResponse>, Error>
where
    C: MetadataClient,
{
    let mut response = FlvDeleteCustomSpusResponse::default();
    let mut results: Vec<FlvResponseMessage> = vec![];

    // look-up custom spus based on their names or ids
    for custom_spu in &request.request.custom_spus {
        let result = match custom_spu {
            FlvCustomSpu::Name(spu_name) => {
                debug!("api request: delete custom-spu with name '{}'", spu_name);

                // spu-name must exist
                if let Some(spu) = &ctx.metadata().spus().spu(spu_name) {
                    delete_custom_spu(ctx, spu).await?
                } else {
                    // spu does not exist
                    FlvResponseMessage::new(
                        spu_name.clone(),
                        FlvErrorCode::SpuNotFound,
                        Some("not found".to_owned()),
                    )
                }
            }
            FlvCustomSpu::Id(spu_id) => {
                debug!("api request: delete custom-spu with id '{}'", spu_id);

                // spu-id must exist
                if let Some(spu) = &ctx.metadata().spus().get_by_id(spu_id) {
                    delete_custom_spu(ctx, spu).await?
                } else {
                    // spu does not exist
                    FlvResponseMessage::new(
                        format!("spu-{}", spu_id),
                        FlvErrorCode::SpuNotFound,
                        Some("not found".to_owned()),
                    )
                }
            }
        };

        // save result
        results.push(result);
    }

    // update response
    response.results = results;
    trace!("flv delete custom-spus resp {:#?}", response);

    Ok(request.new_response(response))
}

/// Generate for delete custom spu operation and return result.
pub async fn delete_custom_spu<C>(
    ctx: &PublicContext<C>,
    spu: &SpuKV,
) -> Result<FlvResponseMessage, Error>
where
    C: MetadataClient,
{
    let spu_name = spu.name();

    // must be Custom Spu
    if !spu.is_custom() {
        return Ok(FlvResponseMessage::new(
            spu_name.clone(),
            FlvErrorCode::SpuError,
            Some("expected 'Custom' spu, found 'Managed' spu".to_owned()),
        ));
    }

    // have have KV context
    let item_ctx = match &spu.kv_ctx().item_ctx {
        Some(ctx) => ctx,
        None => {
            return Ok(FlvResponseMessage::new(
                spu_name.clone(),
                FlvErrorCode::SpuError,
                Some("missing Kv context".to_owned()),
            ))
        }
    };

    // delete custom spec and return result
    let item = item_ctx.as_input();
    match ctx.k8_client().delete_item::<K8SpuSpec, _>(&item).await {
        Ok(_) => Ok(FlvResponseMessage::new_ok(spu_name.clone())),
        Err(err) => Ok(FlvResponseMessage::new(
            spu_name.clone(),
            FlvErrorCode::SpuError,
            Some(err.to_string()),
        )),
    }
}
