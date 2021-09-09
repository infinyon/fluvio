use std::convert::TryInto;
use semver::Version;
use indicatif::{ProgressBar, ProgressStyle};

use fluvio::config::TlsPolicy;

use crate::cli::ClusterCliError;
use crate::start::local::LocalSetupProgressMessage;
use crate::{CheckResults, ClusterError, LocalConfig, LocalInstallError, LocalInstaller, StartStatus};

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
            println!("Successfully installed Fluvio!");
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
            LocalSetupProgressMessage::PreFlighCheck(i) => {
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
