mod api;
mod service_impl;
mod fetch_stream_request;

use tracing::info;

use fluvio_service::FluvioApiServer;
use service_impl::InternalService;

use crate::core::DefaultSharedGlobalContext;

pub use self::fetch_stream_request::FetchStreamRequest;
pub use self::fetch_stream_request::FetchStreamResponse;
pub use self::api::SPUPeerApiEnum;
pub use self::api::SpuPeerRequest;

pub(crate) type InternalApiServer =
    FluvioApiServer<SpuPeerRequest, SPUPeerApiEnum, DefaultSharedGlobalContext, InternalService>;

// start server
pub fn create_internal_server(addr: String, ctx: DefaultSharedGlobalContext) -> InternalApiServer {
    info!(
        "starting SPU: {} at internal service at: {}",
        ctx.local_spu_id(),
        addr
    );

    FluvioApiServer::new(addr, ctx, InternalService::new())
}
