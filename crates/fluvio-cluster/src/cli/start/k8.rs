use std::convert::TryInto;
use fluvio::config::TlsPolicy;
use semver::Version;
use tracing::debug;

use crate::{ClusterInstaller, K8InstallError, StartStatus, ClusterConfig, ClusterError};
use crate::cli::ClusterCliError;
use crate::cli::start::StartOpt;
use crate::check::render::{
    render_statuses_next_steps, render_check_results, render_results_next_steps,
};

pub async fn process_k8(
    opt: StartOpt,
    platform_version: Version,
    upgrade: bool,
    skip_sys: bool,
) -> Result<(), ClusterCliError> {
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
        .chart_values(opt.k8_config.chart_values)
        .render_checks(true)
        .upgrade(upgrade)
        .proxy_addr(opt.proxy_addr)
        .spu_config(opt.spu_config.as_spu_config())
        .connector_prefixes(opt.connector_prefix)
        .with_if(skip_sys, |b| b.install_sys(false))
        .with_if(opt.skip_checks, |b| b.skip_checks(true));

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

    if opt.hide_spinner {
        builder.hide_spinner(true);
    }

    let config = builder.build()?;

    debug!("cluster config: {:#?}", config);
    let installer = ClusterInstaller::from_config(config)?;
    if opt.setup {
        setup_k8(&installer).await?;
    } else {
        let k8_install: Result<_, ClusterError> =
            start_k8(&installer).await.map_err(|err| err.into());
        k8_install?
    }

    Ok(())
}

pub async fn start_k8(installer: &ClusterInstaller) -> Result<(), K8InstallError> {
    match installer.install_fluvio().await {
        // Successfully performed startup without pre-checks
        Ok(StartStatus { checks: None, .. }) => {
            println!("Skipped pre-start checks");
            println!("Successfully installed Fluvio!");
        }
        // Successfully performed startup with pre-checks
        Ok(StartStatus { checks, .. }) => {
            if checks.is_none() {
                println!("Skipped pre-start checks");
            }
        }
        // Aborted startup because pre-checks failed
        Err(K8InstallError::FailedPrecheck(check_statuses)) => {
            render_statuses_next_steps(&check_statuses);
        }
        // Another type of error occurred during checking or startup
        Err(other) => return Err(other),
    }

    Ok(())
}

pub async fn setup_k8(installer: &ClusterInstaller) -> Result<(), ClusterCliError> {
    let check_results = installer.setup().await;
    render_check_results(&check_results);
    render_results_next_steps(&check_results);
    Ok(())
}
