use std::convert::TryInto;
use semver::Version;
use indicatif::{ProgressBar, ProgressStyle};

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::start::local::{LocalInstallProgressMessage, LocalSetupProgressMessage};
use crate::{CheckResults, ClusterError, LocalConfig, LocalInstallError, LocalInstaller};

use super::StartOpt;
use crate::check::render::{RenderedText, render_results_next_steps, render_statuses_next_steps};
use futures_util::StreamExt;

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
        setup_local_with_progress(installer).await?;
    } else {
        install_local_with_progress(installer).await?;
    }

    Ok(())
}

pub async fn install_local_with_progress(installer: LocalInstaller) -> Result<(), ClusterCliError> {
    let progress_bar = ProgressBar::new(0);
    progress_bar.enable_steady_tick(100);

    progress_bar.set_style(
        ProgressStyle::default_bar()
            .progress_chars("##-")
            .template("{spinner:.green} {msg}"),
    );

    let mut progress = installer.install_with_progress().await;
    while let Some(local_progress) = progress.next().await {
        progress_bar.inc(1);
        progress_bar.println(&local_progress.text());
        let mut check_statuses = vec![];
        match local_progress {
            LocalInstallProgressMessage::ClusterError(e) => return Err(e.into()),
            LocalInstallProgressMessage::Check(c) => {
                // If any check results encountered an error, bubble the error
                if c.is_err() {
                    let err: ClusterError = LocalInstallError::PrecheckErrored(vec![c]).into();
                    return Err(err.into());
                }
                if let Ok(status) = c {
                    if let crate::CheckStatus::Fail(_) = status {
                        check_statuses.push(status);
                        render_statuses_next_steps(&check_statuses);
                    }
                }
            }
            _ => (),
        };
    }
    progress_bar.finish_and_clear();

    Ok(())
}

pub async fn setup_local_with_progress(installer: LocalInstaller) -> Result<(), ClusterCliError> {
    use colored::*;
    let progress_bar = ProgressBar::new(1);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .progress_chars("=> ")
            .template(&format!(
                "{:>12} {{spinner:.green}} [{{bar}}] {{pos}}/{{len}} {{msg}}",
                "Checking: ".green().bold()
            )),
    );

    let mut progress = installer.setup_with_progress().await;
    let mut check_results = CheckResults::default();
    while let Some(local_progress) = progress.next().await {
        match local_progress {
            LocalSetupProgressMessage::Check(check_result) => {
                let text = check_result.text();
                if check_result.is_err() {
                    progress_bar.finish_and_clear();
                    println!("{}", text);
                    break;
                }
                check_results.push(check_result);
                progress_bar.inc(1);
                progress_bar.println(&text);
            }
            LocalSetupProgressMessage::PreFlightCheck(i) => {
                println!("{}", "Running pre-flight checks".bold());
                progress_bar.set_length(i);
                progress_bar.enable_steady_tick(100);
            }
        }
    }
    progress_bar.finish_and_clear();
    render_results_next_steps(&check_results);
    Ok(())
}
