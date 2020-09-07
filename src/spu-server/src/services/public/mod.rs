mod api_versions;
mod service_impl;
mod produce_handler;
mod fetch_handler;
mod offset_request;
mod cf_handler;

use tracing::info;

use kf_service::KfApiServer;
use service_impl::PublicService;
use spu_api::server::SpuServerRequest;
use spu_api::server::SpuServerApiKey;

use crate::core::DefaultSharedGlobalContext;
use kf_protocol::api::ReplicaKey;

pub type OffsetReplicaList = std::collections::HashSet<ReplicaKey>;

pub(crate) type PublicApiServer =
    KfApiServer<SpuServerRequest, SpuServerApiKey, DefaultSharedGlobalContext, PublicService>;

// start server
pub fn create_public_server(addr: String, ctx: DefaultSharedGlobalContext) -> PublicApiServer {
    info!(
        "starting SPU: {} at public service at: {}",
        ctx.local_spu_id(),
        addr
    );

    KfApiServer::new(addr, ctx, PublicService::new())
}
