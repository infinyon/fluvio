use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryInto;
use std::process::Command;

use fluvio::config::TlsPolicy;

use crate::{ClusterInstaller, ClusterError, K8InstallError, StartStatus};
use crate::cli::ClusterCliError;
use crate::cli::start::StartOpt;
use crate::check::render::{
    render_statuses_next_steps, render_check_results, render_results_next_steps,
};

pub async fn install_core(
    opt: StartOpt,
    upgrade: bool,
    skip_sys: bool,
) -> Result<(), ClusterCliError> {
    let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;

    let mut builder = ClusterInstaller::new()
        .with_namespace(opt.k8_config.namespace)
        .with_group_name(opt.k8_config.group_name)
        .with_spu_replicas(opt.spu)
        .with_save_profile(!opt.skip_profile_creation)
        .with_tls(client, server)
        .with_chart_values(opt.k8_config.chart_values)
        .with_render_checks(true)
        .with_upgrade(upgrade);

    if skip_sys {
        builder = builder.with_system_chart(false);
    }

    match opt.k8_config.image_version {
        // If an image tag is given, use it
        Some(image_tag) => {
            builder = builder.with_image_tag(image_tag.trim());
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
            builder = builder.with_image_tag(git_hash.trim());
        }
        _ => (),
    }

    match opt.k8_config.chart_location {
        // If a chart location is given, use it
        Some(chart_location) => {
            builder = builder.with_local_chart(chart_location);
        }
        // If we're in develop mode (but no explicit chart location), use hardcoded local path
        None if opt.develop => {
            builder = builder.with_local_chart("./k8-util/helm");
        }
        _ => (),
    }

    match opt.k8_config.registry {
        // If a registry is given, use it
        Some(registry) => {
            builder = builder.with_image_registry(registry);
        }
        None => {
            builder = builder.with_image_registry("infinyon");
        }
    }

    builder = builder.with_chart_version(opt.k8_config.chart_version.to_string());

    if let Some(rust_log) = opt.rust_log {
        builder = builder.with_rust_log(rust_log);
    }

    if let Some(authorization_config_map) = opt.authorization_config_map {
        builder = builder.with_authorization_config_map(authorization_config_map);
    }

    if opt.skip_checks {
        builder = builder.with_skip_checks(true);
    }

    let installer = builder.build()?;
    let results = installer.install_fluvio().await;
    match results {
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

pub async fn run_setup(opt: StartOpt) -> Result<(), ClusterCliError> {
    let mut builder = ClusterInstaller::new().with_namespace(opt.k8_config.namespace);
    match opt.k8_config.chart_location {
        // If a chart location is given, use it
        Some(chart_location) => {
            builder = builder.with_local_chart(chart_location);
        }
        // If we're in develop mode (but no explicit chart location), use local path
        None if opt.develop => {
            builder = builder.with_local_chart("./k8-util/helm");
        }
        _ => (),
    }

    let installer = builder.build()?;
    println!("Performing pre-startup checks...");
    let check_results = installer.setup().await;
    render_check_results(&check_results);
    render_results_next_steps(&check_results);
    Ok(())
}
