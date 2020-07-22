mod private_server;

use log::info;

use private_server::ScInternalService;
use kf_service::KfApiServer;

use crate::core::SharedContext;

// start server
pub fn start_internal_server(ctx: SharedContext) {
    let addr = ctx.config().private_endpoint.clone();
    info!("starting internal services at: {}", addr);

    let server = KfApiServer::new(addr, ctx, ScInternalService::new());
    server.run();
}
