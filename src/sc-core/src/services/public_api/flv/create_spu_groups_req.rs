//!
//! # Create Spu Groups Request
//!
//! Converts Spu Gruups API request into KV request and sends to KV store for processing.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use kf_protocol::api::FlvErrorCode;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::spg::SpuTemplate;
use k8_metadata::spg::StorageConfig;
use k8_metadata::metadata::Env;
use k8_metadata_client::MetadataClient;
use k8_metadata::metadata::Spec as K8Spec;
use k8_metadata::metadata::TemplateSpec;
use sc_api::server::FlvResponseMessage;
use sc_api::server::spu::{FlvCreateSpuGroupsRequest, FlvCreateSpuGroupsResponse};
use sc_api::server::spu::FlvCreateSpuGroupRequest;
use sc_api::server::spu::FlvEnvVar;
use sc_api::server::spu::FlvStorageConfig;

use super::PublicContext;

/// Handler for spu groups request
pub async fn handle_create_spu_groups_request<C>(
    request: RequestMessage<FlvCreateSpuGroupsRequest>,
    ctx: &PublicContext<C>,
) -> Result<ResponseMessage<FlvCreateSpuGroupsResponse>, Error>
where
    C: MetadataClient,
{
    let (header, spu_group_req) = request.get_header_request();

    let mut results: Vec<FlvResponseMessage> = vec![];

    // process create spu groups requests in sequence
    for spu_group in spu_group_req.spu_groups {
        debug!("api request: create spu-group '{}'", spu_group.name);

        let result = process_custom_spu_request(ctx, spu_group).await;
        results.push(result);
    }

    // send response
    let mut response = FlvCreateSpuGroupsResponse::default();
    response.results = results;
    trace!("create spu-group response {:#?}", response);

    Ok(RequestMessage::<FlvCreateSpuGroupsRequest>::response_with_header(&header, response))
}

/// Process custom spu, converts spu spec to K8 and sends to KV store
async fn process_custom_spu_request<C>(
    ctx: &PublicContext<C>,
    group_req: FlvCreateSpuGroupRequest,
) -> FlvResponseMessage
where
    C: MetadataClient,
{
    let (name, spg_spec): (String, SpuGroupSpec) = group_req.to_spec();

    match ctx.create(name.clone(), spg_spec).await {
        Ok(_) => FlvResponseMessage::new_ok(name.clone()),
        Err(err) => {
            let error = Some(err.to_string());
            FlvResponseMessage::new(name, FlvErrorCode::SpuError, error)
        }
    }
}

// convert
trait K8Request<S>
where
    S: K8Spec,
{
    fn to_spec(self) -> (String, S);
}

impl K8Request<SpuGroupSpec> for FlvCreateSpuGroupRequest {
    fn to_spec(self) -> (String, SpuGroupSpec) {
        (
            self.name,
            SpuGroupSpec {
                replicas: self.replicas,
                min_id: self.min_id.clone(),
                template: TemplateSpec::new(SpuTemplate {
                    rack: self.rack.clone(),
                    storage: self.config.storage.map(|cfg| cfg.convert()),
                    env: self
                        .config
                        .env
                        .into_iter()
                        .map(|env| env.convert())
                        .collect(),
                    ..Default::default()
                }),
            },
        )
    }
}

// simplify convert
pub trait Convert<T> {
    fn convert(self) -> T;
}

impl Convert<Env> for FlvEnvVar {
    fn convert(self) -> Env {
        Env::key_value(self.name, self.value)
    }
}

impl Convert<StorageConfig> for FlvStorageConfig {
    fn convert(self) -> StorageConfig {
        StorageConfig {
            log_dir: self.log_dir,
            size: self.size,
        }
    }
}
