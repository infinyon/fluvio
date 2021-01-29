use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryInto;
use std::process::Command;

use fluvio::config::TlsPolicy;

use crate::{ClusterInstaller, ClusterError, K8InstallError, StartStatus, ClusterConfig};
use crate::cli::ClusterCliError;
use crate::cli::start::StartOpt;
use crate::check::render::{
    render_statuses_next_steps, render_check_results, render_results_next_steps,
};

pub async fn process_k8(
    opt: StartOpt,
    default_chart_version: &str,
    upgrade: bool,
    skip_sys: bool,
) -> Result<(), ClusterCliError> {
    let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;

    let chart_version = opt
        .k8_config
        .chart_version
        .as_deref()
        .unwrap_or(default_chart_version);

    let mut builder = ClusterConfig::builder();
    builder
        .namespace(opt.k8_config.namespace)
        .group_name(opt.k8_config.group_name)
        .spu_replicas(opt.spu)
        .save_profile(!opt.skip_profile_creation)
        .tls(client, server)
        .chart_values(opt.k8_config.chart_values)
        .chart_version(chart_version)
        .render_checks(true)
        .upgrade(upgrade)
        .with_if(skip_sys, |b| b.install_sys(false))
        .with_if(opt.skip_checks, |b| b.skip_checks(true));

    match opt.k8_config.chart_location {
        // If a chart location is given, use it
        Some(chart_location) => {
            builder.local_chart(chart_location);
        }
        // If we're in develop mode (but no explicit chart location), use hardcoded local path
        None if opt.develop => {
            builder.local_chart("./k8-util/helm");
        }
        _ => (),
    }

    match opt.k8_config.registry {
        // If a registry is given, use it
        Some(registry) => builder.image_registry(registry),
        None => builder.image_registry("infinyon"),
    };

    if let Some(rust_log) = opt.rust_log {
        builder.rust_log(rust_log);
    }
    if let Some(map) = opt.authorization_config_map {
        builder.authorization_config_map(map);
    }

    match opt.k8_config.image_version {
        // If an image tag is given, use it
        Some(image_tag) => {
            builder.image_tag(image_tag.trim());
        }
        // If we're in develop mode (but no explicit tag), use current git hash
        None if opt.develop => {
            let output = Command::new("git").args(&["rev-parse", "HEAD"]).output()?;
            let git_hash = String::from_utf8(output.stdout).map_err(|e| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("failed to get git hash: {}", e),
                )
            })?;
            builder.image_tag(git_hash.trim());
        }
        _ => (),
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
