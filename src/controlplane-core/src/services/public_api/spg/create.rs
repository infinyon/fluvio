//!
//! # Create Spu Groups Request
//!
//! Converts Spu Gruups API request into KV request and sends to KV store for processing.
//!

use std::io::Error;

use tracing::{debug, trace};

use fluvio_controlplane_api::FlvStatus;
use fluvio_controlplane_api::spg::*;

use crate::core::*;

/// Handler for spu groups request
pub async fn handle_create_spu_group_request(
    name: String,
    spec: SpuGroupSpec,
    _dry_run: bool,
    ctx: SharedContext,
) -> Result<FlvStatus, Error> {
    debug!("creating spu group: {}", name);

    let status = process_custom_spu_request(&ctx, name, spec).await;
    trace!("create spu-group response {:#?}", status);

    Ok(status)
}

/// Process custom spu, converts spu spec to K8 and sends to KV store
async fn process_custom_spu_request(
    ctx: &Context,
    name: String,
    spg_spec: SpuGroupSpec,
) -> FlvStatus {
    use kf_protocol::api::FlvErrorCode;

    if let Err(err) = ctx.spgs().create_spec(name.clone(), spg_spec).await {
        let error = Some(err.to_string());
        FlvStatus::new(name, FlvErrorCode::SpuError, error)
    } else {
        FlvStatus::new_ok(name.clone())
    }
}
