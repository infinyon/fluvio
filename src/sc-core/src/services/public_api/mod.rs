mod public_server;
mod spg;
mod spu;
mod topic;
mod partition;
mod api_version;
mod create;
mod delete;
mod list;
mod watch;

pub use context::*;

mod context {

    use tracing::info;
    use tracing::instrument;

    use kf_service::KfApiServer;

    use crate::core::*;
    use super::public_server::PublicService;

    /// create public server
    #[instrument(
        name = "sc_public_server"
        skip(ctx),
        fields(address = &*ctx.config().public_endpoint)
    )]
    pub fn start_public_server(ctx: SharedContext) {
        let addr = ctx.config().public_endpoint.clone();
        info!("start public api service");

        let server = KfApiServer::new(addr, ctx, PublicService::new());
        server.run();
    }
}
