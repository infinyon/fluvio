use std::sync::Arc;

mod api;
mod service_impl;
mod fetch_stream_request;

use tracing::info;

use fluvio_service::FluvioApiServer;
use service_impl::InternalService;

use crate::core::GlobalContext;
pub use self::fetch_stream_request::FetchStreamRequest;
pub use self::fetch_stream_request::FetchStreamResponse;
pub use self::api::SPUPeerApiEnum;
pub use self::api::SpuPeerRequest;

pub(crate) type InternalApiServer =
    FluvioApiServer<SpuPeerRequest, SPUPeerApiEnum, Arc<GlobalContext>, InternalService>;

// start server
pub fn create_internal_server(addr: String, ctx: Arc<GlobalContext>) -> InternalApiServer {
    info!(
        "starting SPU: {} at internal service at: {}",
        ctx.local_spu_id(),
        addr
    );

    FluvioApiServer::new(addr, ctx, InternalService::new())
}
