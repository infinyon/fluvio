mod api_versions;
mod service_impl;
mod produce_handler;
mod fetch_handler;
mod offset_request;
mod stream_fetch;

use tracing::info;

use fluvio_auth::identity::AuthorizationIdentity;
use fluvio_service::FlvApiServer;
use service_impl::PublicService;
use fluvio_spu_schema::server::SpuServerRequest;
use fluvio_spu_schema::server::SpuServerApiKey;
use dataplane::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;
use crate::services::auth::basic::Policy;

pub type OffsetReplicaList = std::collections::HashSet<ReplicaKey>;

pub(crate) type PublicApiServer = FlvApiServer<
    SpuServerRequest,
    SpuServerApiKey,
    DefaultSharedGlobalContext,
    PublicService,
    AuthorizationIdentity,
    Policy,
>;

// start server
pub fn create_public_server(addr: String, ctx: DefaultSharedGlobalContext) -> PublicApiServer {
    info!(
        "starting SPU: {} at public service at: {}",
        ctx.local_spu_id(),
        addr
    );

    FlvApiServer::new(addr, ctx, PublicService::new())
}
