use fluvio_storage::FileReplica;

use crate::config::{SpuConfig, SpuOpt};
use crate::services::create_internal_server;
use crate::services::create_public_server;
use crate::services::internal::InternalApiServer;
use crate::services::public::PublicApiServer;
use crate::core::DefaultSharedGlobalContext;
use crate::core::GlobalContext;
use crate::control_plane::ScDispatcher;

type FileReplicaContext = GlobalContext<FileReplica>;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn main_loop(opt: SpuOpt) {
    use std::time::Duration;

    use fluvio_future::task::run_block_on;
    use fluvio_future::timer::sleep;
    // parse configuration (program exits on error)
    let (spu_config, tls_acceptor_option) = opt.process_spu_cli_or_exit();

    println!("starting spu server (id:{})", spu_config.id);

    run_block_on(async move {
        let (_ctx, internal_server, public_server) =
            create_services(spu_config.clone(), true, true);

        let _public_shutdown = internal_server.unwrap().run();
        let _private_shutdown = public_server.unwrap().run();

        if let Some(tls_config) = tls_acceptor_option {
            proxy::start_proxy(spu_config, tls_config).await;
        }

        println!("SPU Version: {} started successfully", VERSION);

        // infinite loop
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    });
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

    let public_ep_addr = ctx.config().public_socket_addr().to_owned();
    let private_ep_addr = ctx.config().private_socket_addr().to_owned();

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

    let sc_dispatcher = ScDispatcher::new(ctx.clone(), ctx.config().peer_max_bytes);
    sc_dispatcher.run();

    (ctx, internal_server, public_server)
}

mod proxy {

    use std::process;

    use tracing::info;

    use flv_util::print_cli_err;
    use fluvio_future::openssl::TlsAcceptor;
    use crate::config::SpuConfig;
    use flv_tls_proxy::start as proxy_start;

    pub async fn start_proxy(config: SpuConfig, acceptor: (TlsAcceptor, String)) {
        let (tls_acceptor, proxy_addr) = acceptor;
        let target = config.public_endpoint;
        info!("starting TLS proxy: {}", proxy_addr);

        if let Err(err) = proxy_start(&proxy_addr, tls_acceptor, target).await {
            print_cli_err!(err);
            process::exit(-1);
        } else {
            info!("TLS started successfully");
            println!("TLS proxy started");
        }
    }
}
