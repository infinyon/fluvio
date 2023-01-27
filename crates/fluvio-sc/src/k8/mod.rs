//!
//! # Initialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

mod controllers;
mod objects;
mod migration;

#[cfg(test)]
mod fixture;

use k8_client::new_shared;
use tracing::{info, error};

use crate::cli::ScOpt;

pub fn main_k8_loop(opt: ScOpt) {
    use std::time::Duration;

    use fluvio_future::task::run_block_on;
    use fluvio_future::timer::sleep;

    use crate::init::start_main_loop;
    use controllers::run_k8_operators;

    // parse configuration (program exits on error)
    let is_local = opt.is_local();
    println!("CLI Option: {opt:#?}");
    let ((sc_config, auth_policy), k8_config, tls_option) = opt.parse_cli_or_exit();

    println!("Starting SC, platform: {}", crate::VERSION);

    inspect_system();

    run_block_on(async move {
        info!("initializing k8 client");
        // init k8 service
        let k8_client = new_shared(k8_config).expect("problem creating k8 client");
        let namespace = sc_config.namespace.clone();

        if let Err(err) =
            migration::SmartModuleMigrationController::migrate(k8_client.clone(), &namespace).await
        {
            error!("migration failed: {:#?}", err);
        }

        info!("starting main loop");
        let ctx = start_main_loop((sc_config.clone(), auth_policy), k8_client.clone()).await;

        if !is_local {
            run_k8_operators(
                namespace.clone(),
                k8_client,
                ctx,
                tls_option.clone().map(|(_, config)| config),
            )
            .await;
        }

        if let Some((proxy_port, tls_config)) = tls_option {
            let tls_acceptor = tls_config
                .try_build_tls_acceptor()
                .expect("can't build tls acceptor");
            proxy::start_proxy(sc_config, (tls_acceptor, proxy_port)).await;
        }

        println!("Streaming Controller started successfully");

        // do infinite loop
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    });
}

/// print out system information
fn inspect_system() {
    use sysinfo::System;
    use sysinfo::SystemExt;

    let mut sys = System::new_all();
    sys.refresh_all();
    info!(version = crate::VERSION, "Platform");
    info!(commit = env!("GIT_HASH"), "Git");
    info!(name = ?sys.name(),"System");
    info!(kernel = ?sys.kernel_version(),"System");
    info!(os_version = ?sys.long_os_version(),"System");
    info!(core_count = ?sys.physical_core_count(),"System");
    info!(total_memory = sys.total_memory(), "System");
    info!(available_memory = sys.available_memory(), "System");
    info!(uptime = sys.uptime(), "Uptime in secs");
}

mod proxy {
    use std::process;
    use tracing::info;

    use fluvio_types::print_cli_err;
    pub use fluvio_future::openssl::TlsAcceptor;

    use fluvio_auth::x509::X509Authenticator;
    use flv_tls_proxy::{
        start as proxy_start, start_with_authenticator as proxy_start_with_authenticator,
    };

    use crate::config::ScConfig;

    pub async fn start_proxy(config: ScConfig, acceptor: (TlsAcceptor, String)) {
        let (tls_acceptor, proxy_addr) = acceptor;
        let target = config.public_endpoint;
        info!("starting TLS proxy: {}", proxy_addr);

        let result = if let Some(x509_auth_scopes) = config.x509_auth_scopes {
            let authenticator = Box::new(X509Authenticator::new(&x509_auth_scopes));
            proxy_start_with_authenticator(&proxy_addr, tls_acceptor, target, authenticator).await
        } else {
            proxy_start(&proxy_addr, tls_acceptor, target).await
        };

        if let Err(err) = result {
            print_cli_err!(err);
            process::exit(-1);
        }
    }
}
