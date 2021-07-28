use std::convert::TryInto;
use fluvio::config::TlsPolicy;
use semver::Version;

use crate::{ClusterInstaller, ClusterError, K8InstallError, StartStatus, ClusterConfig};
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

    let config = builder.build()?;
    let installer = ClusterInstaller::from_config(config)?;
    if opt.setup {
        setup_k8(&installer).await?;
    } else {
        start_k8(&installer).await?;
    }

    Ok(())
}

pub async fn start_k8(installer: &ClusterInstaller) -> Result<(), ClusterCliError> {
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
            println!("Successfully installed Fluvio!");
        }
        // Aborted startup because pre-checks failed
        Err(ClusterError::InstallK8(K8InstallError::FailedPrecheck(check_statuses))) => {
            render_statuses_next_steps(&check_statuses);
        }
        // Another type of error occurred during checking or startup
        Err(other) => return Err(other.into()),
    }

    Ok(())
}

pub async fn setup_k8(installer: &ClusterInstaller) -> Result<(), ClusterCliError> {
    println!("Performing pre-startup checks...");
    let check_results = installer.setup().await;
    render_check_results(&check_results);
    render_results_next_steps(&check_results);
    Ok(())
}
