mod api_versions;
mod service_impl;
mod produce_handler;
mod fetch_handler;
mod offset_request;
mod stream_fetch;

use tracing::info;

use kf_service::KfApiServer;
use service_impl::PublicService;
use fluvio_spu_schema::server::SpuServerRequest;
use fluvio_spu_schema::server::SpuServerApiKey;
use dataplane_protocol::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;


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
