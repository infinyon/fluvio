use std::convert::TryInto;

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::{LocalClusterInstaller, ClusterError, LocalInstallError, StartStatus};

use super::StartOpt;
use crate::cli::check::{
    render_check_statuses, render_statuses_next_steps, render_check_results,
    render_results_next_steps,
};

/// Attempts to start a local Fluvio cluster
///
/// Returns `Ok(true)` on success, `Ok(false)` if pre-checks failed and are
/// reported, or `Err(e)` if something unexpected occurred.
pub async fn install_local(opt: StartOpt) -> Result<(), ClusterCliError> {
    let mut builder = LocalClusterInstaller::new()
        .with_log_dir(opt.log_dir.to_string())
        .with_spu_replicas(opt.spu);

    match opt.k8_config.chart_location {
        Some(chart_location) => {
            builder = builder.with_local_chart(chart_location);
        }
        None if opt.develop => {
            builder = builder.with_local_chart("./k8-util/helm");
        }
        _ => (),
    }

    if let Some(rust_log) = opt.rust_log {
        builder = builder.with_rust_log(rust_log);
    }

    if opt.tls.tls {
        let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;
        builder = builder.with_tls(client, server);
    }

    if opt.skip_checks {
        builder = builder.with_skip_checks(true);
    }

    let installer = builder.build()?;
    let install_result = installer.install().await;

    match install_result {
        // Successfully performed startup without pre-checks
        Ok(StartStatus { checks: None, .. }) => {
            println!("Skipped pre-start checks");
            println!("Successfully installed Fluvio!");
        }
        // Successfully performed startup with pre-checks
        Ok(StartStatus {
            checks: Some(check_statuses),
            ..
        }) => {
            render_check_statuses(&check_statuses);
        }
        // Aborted startup because pre-checks failed
        Err(ClusterError::InstallLocal(LocalInstallError::FailedPrecheck(check_statuses))) => {
            render_check_statuses(&check_statuses);
            render_statuses_next_steps(&check_statuses);
        }
        // Another type of error occurred during checking or startup
        Err(other) => return Err(other.into()),
    }

    Ok(())
}

pub async fn run_local_setup(_opt: StartOpt) -> Result<(), ClusterCliError> {
    let installer = LocalClusterInstaller::new().build()?;
    let check_results = installer.setup().await;
    render_check_results(&check_results);
    render_results_next_steps(&check_results);
    Ok(())
}
