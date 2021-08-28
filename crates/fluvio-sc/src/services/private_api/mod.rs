mod private_server;

use tracing::info;
use tracing::instrument;

use private_server::ScInternalService;
use fluvio_service::FluvioApiServer;

use crate::core::SharedContext;

// start server
#[instrument(
    name = "sc_private_server"
    skip(ctx),
    fields(address = &*ctx.config().private_endpoint)
)]
pub fn start_internal_server(ctx: SharedContext) {
    info!("starting internal services");

    let addr = ctx.config().private_endpoint.clone();
    let server = FluvioApiServer::new(addr, ctx, ScInternalService::new());
    server.run();
}
