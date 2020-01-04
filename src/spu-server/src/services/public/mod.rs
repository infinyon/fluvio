mod api_versions;
mod service_impl;
mod produce_handler;
mod fetch_handler;
mod local_spu_request;
mod offset_request;
mod cf_handler;

use log::info;
use std::net::SocketAddr;

use kf_service::KfApiServer;
use service_impl::PublicService;
use spu_api::PublicRequest;
use spu_api::SpuApiKey;

use crate::core::DefaultSharedGlobalContext;

pub(crate) type PublicApiServer = KfApiServer<
    PublicRequest,
    SpuApiKey,
    DefaultSharedGlobalContext,
    PublicService>;

// start server
pub fn create_public_server(addr: SocketAddr, ctx: DefaultSharedGlobalContext) -> PublicApiServer
{
    info!("starting SPU: {} at public service at: {}", ctx.local_spu_id(),addr);

    KfApiServer::new(addr, ctx, PublicService::new())
}
