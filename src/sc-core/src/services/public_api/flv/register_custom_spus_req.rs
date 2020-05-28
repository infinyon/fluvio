//!
//! # Create Custom Spus Request
//!
//! Converts Custom Spu API request into KV request and sends to KV store for processing.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use kf_protocol::api::FlvErrorCode;

use k8_metadata::metadata::ObjectMeta;
use k8_metadata_client::MetadataClient;
use flv_metadata::spu::{SpuSpec, Endpoint, SpuType, IngressPort};

use sc_api::FlvResponseMessage;
use sc_api::spu::{FlvRegisterCustomSpusRequest, FlvRegisterCustomSpusResponse};
use sc_api::spu::FlvRegisterCustomSpuRequest;

use crate::core::LocalStores;
use crate::core::spus::SpuKV;
use crate::core::common::KvContext;

use super::PublicContext;

/// Handler for create spus request
pub async fn handle_register_custom_spus_request<C>(
    request: RequestMessage<FlvRegisterCustomSpusRequest>,
    ctx: &PublicContext<C>,
) -> Result<ResponseMessage<FlvRegisterCustomSpusResponse>, Error>
where
    C: MetadataClient,
{
    let (header, custom_spu_req) = request.get_header_request();

    let mut response = FlvRegisterCustomSpusResponse::default();
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
        let result = process_custom_spu_request(ctx, custom_spu_req).await;
        results.push(result);
    }

    // send response
    response.results = results;
    trace!("create custom-spus response {:#?}", response);

    Ok(RequestMessage::<FlvRegisterCustomSpusRequest>::response_with_header(&header, response))
}

/// Validate custom_spu requests (one at a time)
fn validate_custom_spu_request(
    custom_spu_req: &FlvRegisterCustomSpuRequest,
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
async fn process_custom_spu_request<C>(
    ctx: &PublicContext<C>,
    custom_spu_req: FlvRegisterCustomSpuRequest,
) -> FlvResponseMessage
where
    C: MetadataClient,
{
    let name = custom_spu_req.name.clone();

    if let Err(err) = register_custom_spu(ctx, custom_spu_req).await {
        let error = Some(err.to_string());
        FlvResponseMessage::new(name, FlvErrorCode::SpuError, error)
    } else {
        FlvResponseMessage::new_ok(name)
    }
}

async fn register_custom_spu<C>(
    ctx: &PublicContext<C>,
    spu_req: FlvRegisterCustomSpuRequest,
) -> Result<(), C::MetadataClientError>
where
    C: MetadataClient,
{
    let meta = ObjectMeta::new(spu_req.name.clone(), ctx.namespace.clone());
    let public_endpoint =
        IngressPort::from_port_host(spu_req.public_server.port, spu_req.public_server.host);
    let private_endpoint =
        Endpoint::from_port_host(spu_req.private_server.port, spu_req.private_server.host);
    let spu_spec = SpuSpec {
        id: spu_req.id,
        spu_type: SpuType::Custom,
        public_endpoint,
        private_endpoint,
        rack: spu_req.rack.clone(),
    };
    let kv_ctx = KvContext::default().with_ctx(meta);
    let custom_spu_kv = SpuKV::new_with_context(spu_req.name.clone(), spu_spec, kv_ctx);

    ctx.k8_ws().add(custom_spu_kv).await
}
