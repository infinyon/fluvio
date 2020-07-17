mod public_server;
mod spg;
mod spu;
mod topic;
mod partition;
mod metadata;
mod api_version;
mod create;
mod delete;
mod list;

pub use context::*;

mod context {

    use log::info;

    use kf_service::KfApiServer;

    use crate::core::*;
    use super::public_server::PublicService;


    /// create public server
    pub fn start_public_server(ctx: SharedContext) {
        let addr = ctx.config().public_endpoint.clone();
        info!("start public api service at: {}", addr);

        let server = KfApiServer::new(addr, ctx, PublicService::new());
        server.run();
    }
}
