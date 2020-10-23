mod api;
mod fetch_stream;
mod service_impl;
mod fetch_stream_request;

use tracing::info;

use fluvio_auth::identity::AuthorizationIdentity;
use fluvio_service::FlvApiServer;
use service_impl::InternalService;

use crate::core::DefaultSharedGlobalContext;
use crate::services::auth::basic::Policy;

pub use self::fetch_stream_request::FetchStreamRequest;
pub use self::fetch_stream_request::FetchStreamResponse;
pub use self::api::SPUPeerApiEnum;
pub use self::api::SpuPeerRequest;

pub(crate) type InternalApiServer = FlvApiServer<
    SpuPeerRequest,
    SPUPeerApiEnum,
    DefaultSharedGlobalContext,
    InternalService,
    AuthorizationIdentity,
    Policy,
>;

// start server
pub fn create_internal_server(addr: String, ctx: DefaultSharedGlobalContext) -> InternalApiServer {
    info!(
        "starting SPU: {} at internal service at: {}",
        ctx.local_spu_id(),
        addr
    );

    FlvApiServer::new(addr, ctx, InternalService::new())
}
