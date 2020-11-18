use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryInto;
use std::process::Command;

use fluvio_cluster::ClusterInstaller;
use fluvio::config::TlsPolicy;
use super::*;

pub async fn install_core(opt: InstallOpt) -> Result<(), CliError> {
    let (client, server): (TlsPolicy, TlsPolicy) = opt.tls.try_into()?;

    let mut builder = ClusterInstaller::new()
        .with_namespace(opt.k8_config.namespace)
        .with_group_name(opt.k8_config.group_name)
        .with_spu_replicas(opt.spu)
        .with_save_profile(!opt.skip_profile_creation)
        .with_tls(client, server);

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
    installer.install_fluvio().await?;
    Ok(())
}

pub fn install_sys(opt: InstallOpt) -> Result<(), CliError> {
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
    installer._install_sys()?;
    println!("fluvio sys chart has been installed");
    Ok(())
}
