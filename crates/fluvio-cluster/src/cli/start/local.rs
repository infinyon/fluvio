use std::convert::TryInto;
use semver::Version;

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::{LocalInstaller, ClusterError, LocalInstallError, StartStatus, LocalConfig};

use super::StartOpt;

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

    if let Some(chart_location) = opt.k8_config.chart_location {
        builder.local_chart(chart_location);
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

    if opt.hide_spinner {
        builder.hide_spinner(true);
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
    installer.install().await?;
    Ok(())
}

pub async fn setup_local(installer: &LocalInstaller) -> Result<(), ClusterCliError> {
    let check_results = installer.preflight_check().await;
    render_results_next_steps(&check_results);
    Ok(())
}
