use std::{sync::Arc, path::PathBuf};

use tracing::info;

use k8_client::new_shared;
use k8_client::memory::MemoryClient;
use k8_metadata_client::{SharedClient, MetadataClient};

use crate::{
    cli::{ScOpt, TlsConfig},
    services::auth::basic::BasicRbacPolicy,
    config::ScConfig,
};

pub fn main_loop(opt: ScOpt) {
    // parse configuration (program exits on error)
    println!("CLI Option: {opt:#?}");
    let is_local = opt.is_local();

    inspect_system();
    println!("Starting SC, platform: {}", crate::VERSION);

    if let Some(read_only_path) = opt.read_only().clone() {
        info!("Running in read only mode");
        let ((sc_config, auth_policy), tls_option) = opt.parse_cli_or_exit_read_only();

        info!("initializing metadata from read only configuration");
        let client = Arc::new(
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async { create_memory_client(read_only_path).await })
                .expect("failed to initialize metadata from read only configuration"),
        );
        inner_main_loop(is_local, sc_config, client, auth_policy, tls_option)
    } else {
        info!("Running with K8");

        let ((sc_config, auth_policy), k8_config, tls_option) = opt.parse_cli_or_exit();
        let client = new_shared(k8_config).expect("failed to create k8 client");
        inner_main_loop(is_local, sc_config, client, auth_policy, tls_option)
    };
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

fn inner_main_loop<C>(
    is_local: bool,
    sc_config: ScConfig,
    client: SharedClient<C>,
    auth_policy: Option<BasicRbacPolicy>,
    tls_option: Option<(String, TlsConfig)>,
) where
    C: MetadataClient + 'static,
{
    use std::time::Duration;

    use tokio::time::sleep;

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        info!("initializing client");
        // init k8 service

        let namespace = sc_config.namespace.clone();

        info!("starting main loop");

        let ctx: crate::core::K8SharedContext =
            crate::init::start_main_loop((sc_config.clone(), auth_policy), client.clone()).await;

        if !is_local {
            crate::k8::controllers::run_k8_operators(
                namespace.clone(),
                client,
                ctx.clone(),
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

async fn create_memory_client(path: PathBuf) -> anyhow::Result<MemoryClient> {
    use std::ops::Deref;
    use fluvio_sc_schema::edge::EdgeMetadataFile;

    let metadata_file = EdgeMetadataFile::open(path)?;
    let config = metadata_file.deref();
    let client = MemoryClient::default();

    info!(topics = config.topics.len(), "loading topics");
    for value in &config.topics {
        info!(name = value.metadata.name, "read topic");
        client.create_item(value.as_input()).await?;
    }

    Ok(client)
}
