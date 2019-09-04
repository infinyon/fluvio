//!
//! # Create Custom Spus Request
//!
//! Converts Custom Spu API request into KV request and sends to KV store for processing.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use kf_protocol::api::FlvErrorCode;

use k8_metadata::core::metadata::ObjectMeta;
use metadata::spu::{SpuSpec, Endpoint, SpuType};

use sc_api::FlvResponseMessage;
use sc_api::spu::{FlvCreateCustomSpusRequest, FlvCreateCustomSpusResponse};
use sc_api::spu::FlvCreateCustomSpuRequest;

use crate::ScServerError;
use crate::core::LocalStores;
use crate::core::spus::SpuKV;
use crate::core::common::KvContext;

use super::PublicContext;

/// Handler for create spus request
pub async fn handle_create_custom_spus_request(
    request: RequestMessage<FlvCreateCustomSpusRequest>,
    ctx: &PublicContext,
) -> Result<ResponseMessage<FlvCreateCustomSpusResponse>, Error> {
    let (header, custom_spu_req) = request.get_header_request();

    let mut response = FlvCreateCustomSpusResponse::default();
    let mut results: Vec<FlvResponseMessage> = vec![];

    // process create custom spus requests in sequence
    for custom_spu_req in custom_spu_req.custom_spus {
        debug!(
            "api request: create custom-spu '{}({})'",
            custom_spu_req.name, custom_spu_req.id
        );

        // validate custom-spu request
        if let Err(err_msg) = validate_custom_spu_request(&custom_spu_req, ctx.metadata()) {
            results.push(err_msg);
            continue;
        }

        // process custom-spu request
        let result = process_custom_spu_request(ctx, &custom_spu_req).await;
        results.push(result);
    }

    // send response
    response.results = results;
    trace!("create custom-spus response {:#?}", response);

    Ok(RequestMessage::<FlvCreateCustomSpusRequest>::response_with_header(&header, response))
}

/// Validate custom_spu requests (one at a time)
fn validate_custom_spu_request(
    custom_spu_req: &FlvCreateCustomSpuRequest,
    metadata: &LocalStores,
) -> Result<(), FlvResponseMessage> {
    let spu_id = &custom_spu_req.id;
    let spu_name = &custom_spu_req.name;

    debug!("validating custom-spu: {}({})", spu_name, spu_id);

    // look-up SPU by name or id to check if already exists
    if metadata.spus().spu(spu_name).is_some() || metadata.spus().get_by_id(spu_id).is_some() {
        Err(FlvResponseMessage::new(
            spu_name.clone(),
            FlvErrorCode::SpuAlreadyExists,
            Some(format!("spu '{}({})' already defined", spu_name, spu_id)),
        ))
    } else {
        Ok(())
    }
}

/// Process custom spu, converts spu spec to K8 and sends to KV store
async fn process_custom_spu_request(
    ctx: &PublicContext,
    custom_spu_req: &FlvCreateCustomSpuRequest,
) -> FlvResponseMessage {
    let name = &custom_spu_req.name;

    if let Err(err) = create_custom_spu(ctx, custom_spu_req).await {
        let error = Some(err.to_string());
        FlvResponseMessage::new(name.clone(), FlvErrorCode::SpuError, error)
    } else {
        FlvResponseMessage::new_ok(name.clone())
    }
}

async fn create_custom_spu(
    ctx: &PublicContext,
    spu_req: &FlvCreateCustomSpuRequest,
) -> Result<(), ScServerError> {
    let meta = ObjectMeta::new(spu_req.name.clone(), ctx.namespace.clone());
    let public_ep =
        Endpoint::from_port_host(spu_req.public_server.port, &spu_req.public_server.host);
    let private_ep =
        Endpoint::from_port_host(spu_req.private_server.port, &spu_req.private_server.host);
    let spu_spec = SpuSpec {
        id: spu_req.id,
        spu_type: SpuType::Custom,
        public_endpoint: public_ep,
        private_endpoint: private_ep,
        rack: spu_req.rack.clone(),
    };
    let kv_ctx = KvContext::default().with_ctx(meta);
    let custom_spu_kv = SpuKV::new_with_context(spu_req.name.clone(), spu_spec, kv_ctx);

    ctx.k8_ws().add(custom_spu_kv).await
}
