use std::process::Command;
use std::io::Error as IoError;
use std::io::ErrorKind;
use super::*;
use fluvio_cluster::ClusterInstaller;

pub async fn install_core(opt: InstallCommand) -> Result<(), CliError> {
    let mut builder = ClusterInstaller::new()
        .with_namespace(opt.k8_config.namespace)
        .with_group_name(opt.k8_config.group_name)
        .with_spu_replicas(opt.spu)
        .with_save_profile(!opt.skip_profile_creation);

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
            builder = builder.with_local_chart("./k8-util/helm/fluvio-app");
        }
        _ => (),
    }

    match opt.k8_config.registry {
        // If a registry is given, use it
        Some(registry) => {
            builder = builder.with_image_registry(registry);
        }
        // If we're in develop mode (but no explicit registry), use localhost:5000 registry
        None if opt.develop => {
            builder = builder.with_image_registry("localhost:5000/infinyon");
        }
        _ => (),
    }

    if let Some(chart_version) = opt.k8_config.chart_version {
        builder = builder.with_chart_version(chart_version);
    }

    if let Some(rust_log) = opt.rust_log {
        builder = builder.with_rust_log(rust_log);
    }

    let installer = builder.build()?;
    installer.install_fluvio().await?;
    Ok(())
}

pub fn install_sys(opt: InstallCommand) -> Result<(), CliError> {
    let installer = ClusterInstaller::new()
        .with_namespace(opt.k8_config.namespace)
        .build()?;
    installer._install_sys()?;
    println!("fluvio sys chart has been installed");
    Ok(())
}
