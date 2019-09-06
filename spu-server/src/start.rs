use log::debug;

use future_helper::run;
use storage::FileReplica;

use crate::config::process_spu_cli_or_exit;
use crate::config::SpuConfig;
use crate::services::create_internal_server;
use crate::services::create_public_server;
use crate::services::internal::InternalApiServer;
use crate::services::public::PublicApiServer;
use crate::core::DefaultSharedGlobalContext;
use crate::core::GlobalContext;
use crate::controllers::sc::ScDispatcher;

type FileReplicaContext = GlobalContext<FileReplica>;


const VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub fn main_loop() {
    // parse configuration (program exits on error)
    let spu_config = process_spu_cli_or_exit();

    println!(
        "starting {}-spu services (id:{})",
        spu_config.type_label(),
        spu_config.id
    );

    debug!("spu config: {:#?}",spu_config);

    run(async {
        let (_ctx, internal_server, public_server) = create_services(spu_config, true, true);

        let _public_shutdown = internal_server.unwrap().run();
        let _private_shutdown = public_server.unwrap().run();
    });

    println!("SPU Version: {} started successfully",VERSION);

}

/// create server and spin up services, but don't run server
pub fn create_services(
    local_spu: SpuConfig,
    internal: bool,
    public: bool,
) -> (
    DefaultSharedGlobalContext,
    Option<InternalApiServer>,
    Option<PublicApiServer>,
) {
    let ctx = FileReplicaContext::new_shared_context(local_spu);

    let public_ep_addr = ctx.config().public_socket_addr().clone();
    let private_ep_addr = ctx.config().private_socket_addr().clone();

    let public_server = if public {
        Some(create_public_server(public_ep_addr, ctx.clone()))
    } else {
        None
    };

    let internal_server = if internal {
        Some(create_internal_server(private_ep_addr, ctx.clone()))
    } else {
        None
    };

  
    let sc_dispatcher = ScDispatcher::new(ctx.clone());
 
    sc_dispatcher.run();

    (ctx, internal_server, public_server)
}
