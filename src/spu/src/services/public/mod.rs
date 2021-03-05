mod api_versions;
mod service_impl;
mod produce_handler;
mod fetch_handler;
mod offset_request;
mod stream_fetch;

use tracing::info;

use fluvio_service::FlvApiServer;
use service_impl::PublicService;
use fluvio_spu_schema::server::SpuServerRequest;
use fluvio_spu_schema::server::SpuServerApiKey;


use crate::core::DefaultSharedGlobalContext;
pub use stream_fetch::publishers::StreamPublishers;


pub(crate) type PublicApiServer =
    FlvApiServer<SpuServerRequest, SpuServerApiKey, DefaultSharedGlobalContext, PublicService>;

// start server
pub fn create_public_server(addr: String, ctx: DefaultSharedGlobalContext) -> PublicApiServer {
    info!(
        "starting SPU: {} at public service at: {}",
        ctx.local_spu_id(),
        addr
    );

    FlvApiServer::new(addr, ctx, PublicService::new())
}
