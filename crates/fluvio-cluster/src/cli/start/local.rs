use std::convert::TryInto;
use semver::Version;

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::{LocalInstaller, ClusterError, LocalInstallError, StartStatus, LocalConfig};

use super::StartOpt;
use crate::check::render::{render_statuses_next_steps, render_results_next_steps};

/// Attempts to start a local Fluvio cluster
///
/// Returns `Ok(true)` on success, `Ok(false)` if pre-checks failed and are
/// reported, or `Err(e)` if something unexpected occurred.
pub async fn process_local(
    opt: StartOpt,
    platform_version: Version,
) -> Result<(), ClusterCliError> {
    let mut builder = LocalConfig::builder(platform_version);
    builder
        .log_dir(opt.log_dir.to_string())
        .render_checks(true)
        .spu_replicas(opt.spu);

    match opt.k8_config.chart_location {
        Some(chart_location) => {
            builder.local_chart(chart_location);
        }
        None if opt.develop => {
            builder.local_chart("./k8-util/helm");
        }
        _ => (),
    }

    if let Some(rust_log) = opt.rust_log {
        builder.rust_log(rust_log);
    }

    if opt.tls.tls {
        let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;
        builder.tls(client, server);
    }

    if opt.skip_checks {
        builder.skip_checks(true);
    }

    let config = builder.build()?;
    let installer = LocalInstaller::from_config(config);
    if opt.setup {
        setup_local(&installer).await?;
    } else {
        install_local(&installer).await?;
    }

    Ok(())
}

pub async fn install_local(installer: &LocalInstaller) -> Result<(), ClusterCliError> {
    match installer.install().await {
        // Successfully performed startup
        Ok(StartStatus { checks, .. }) => {
            if checks.is_none() {
                println!("Skipped pre-start checks");
            }
        }
        // Aborted startup because pre-checks failed
        Err(ClusterError::InstallLocal(LocalInstallError::FailedPrecheck(check_statuses))) => {
            render_statuses_next_steps(&check_statuses);
        }
        // Another type of error occurred during checking or startup
        Err(other) => return Err(other.into()),
    }
    Ok(())
}

pub async fn setup_local(installer: &LocalInstaller) -> Result<(), ClusterCliError> {
    let check_results = installer.setup().await;
    render_results_next_steps(&check_results);
    Ok(())
}
