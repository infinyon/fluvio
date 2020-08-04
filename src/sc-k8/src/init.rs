//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

use flv_future_aio::task::main;

use k8_client::new_shared;
use flv_sc_core::start_main_loop;
use crate::{ScOpt, operator::run_k8_operators};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub fn main_k8_loop(opt: ScOpt) {
    // parse configuration (program exits on error)
    let (sc_config, k8_config, tls_option) = opt.parse_cli_or_exit();

    println!("starting sc server with k8: {}", VERSION);

    main(async move {
        // init k8 service
        let k8_client = new_shared(k8_config).expect("problem creating k8 client");
        let namespace = sc_config.namespace.clone();
        let ctx = start_main_loop(sc_config.clone(), k8_client.clone()).await;

        run_k8_operators(
            namespace.clone(),
            k8_client,
            ctx,
            tls_option.clone().map(|(_, config)| config),
        );

        if let Some((proxy_port, tls_config)) = tls_option {
            let tls_acceptor = tls_config
                .try_build_tls_acceptor()
                .expect("can't build tls acceptor");
            proxy::start_proxy(sc_config, (tls_acceptor, proxy_port)).await;
        }

        println!("Streaming Controller started successfully");
    });
}

mod proxy {

    use std::process;

    use log::info;

    use flv_types::print_cli_err;
    use flv_future_aio::net::tls::TlsAcceptor;
    use flv_sc_core::config::ScConfig;
    use flv_tls_proxy::start as proxy_start;

    pub async fn start_proxy(config: ScConfig, acceptor: (TlsAcceptor, String)) {
        let (tls_acceptor, proxy_addr) = acceptor;
        let target = config.public_endpoint;
        info!("starting TLS proxy: {}", proxy_addr);

        if let Err(err) = proxy_start(&proxy_addr, tls_acceptor, target).await {
            print_cli_err!(err);
            process::exit(-1);
        }
    }
}
