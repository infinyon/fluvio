use std::convert::TryInto;

use anyhow::Result;
use fluvio::config::TlsPolicy;
use semver::Version;
use tracing::debug;

use crate::{ClusterInstaller, ClusterConfig};
use crate::cli::start::StartOpt;

pub async fn process_k8(opt: StartOpt, platform_version: Version, upgrade: bool) -> Result<()> {
    let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;

    let mut builder = ClusterConfig::builder(platform_version);

    if opt.develop {
        builder.development()?;
    }

    builder
        .namespace(opt.k8_config.namespace)
        .chart_version(opt.k8_config.chart_version)
        .group_name(opt.k8_config.group_name)
        .spu_replicas(opt.spu)
        .save_profile(!opt.skip_profile_creation)
        .tls(client, server)
        .tls_client_secret_name(opt.k8_config.tls_client_secret_name)
        .tls_server_secret_name(opt.k8_config.tls_server_secret_name)
        .chart_values(opt.k8_config.chart_values)
        .hide_spinner(false)
        .upgrade(upgrade)
        .spu_config(opt.spu_config.as_spu_config())
        .connector_prefixes(opt.connector_prefix)
        .with_if(opt.skip_checks, |b| b.skip_checks(true))
        .use_k8_port_forwarding(opt.k8_config.use_k8_port_forwarding);

    if cfg!(target_os = "macos") {
        builder.proxy_addr(opt.proxy_addr.unwrap_or_else(|| String::from("localhost")));
    } else {
        builder.proxy_addr(opt.proxy_addr);
    }

    if let Some(chart_location) = opt.k8_config.chart_location {
        builder.local_chart(chart_location);
    }

    if let Some(registry) = opt.k8_config.registry {
        builder.image_registry(registry);
    }

    if let Some(rust_log) = opt.rust_log {
        builder.rust_log(rust_log);
    }

    if let Some(map) = opt.authorization_config_map {
        builder.authorization_config_map(map);
    }

    if let Some(image_tag) = opt.k8_config.image_version {
        builder.image_tag(image_tag.trim());
    }

    if let Some(service_type) = opt.service_type {
        builder.service_type(service_type);
    }

    let config = builder.build()?;

    debug!("cluster config: {:#?}", config);
    let installer = ClusterInstaller::from_config(config)?;
    if opt.setup {
        setup_k8(&installer).await?;
    } else {
        start_k8(&installer).await?;
    }

    Ok(())
}

pub async fn start_k8(installer: &ClusterInstaller) -> Result<()> {
    installer.install_fluvio().await?;
    Ok(())
}

pub async fn setup_k8(installer: &ClusterInstaller) -> Result<()> {
    installer.preflight_check(false).await?;
    Ok(())
}
