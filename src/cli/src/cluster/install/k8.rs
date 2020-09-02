use std::process::Command;
use std::io::Error as IoError;
use semver::Version;
use k8_client::K8Config;
use k8_config::KubeContext;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::str::FromStr;
use url::Url;

use super::*;
use fluvio_cluster::ClusterInstaller;

fn get_cluster_server_host(kc_config: KubeContext) -> Result<String, IoError> {
    if let Some(ctx) = kc_config.config.current_cluster() {
        let server_url = ctx.cluster.server.to_owned();
        let url = match Url::parse(&server_url) {
            Ok(url) => url,
            Err(e) => {
                return Err(IoError::new(
                    ErrorKind::Other,
                    format!("error parsing server url {}", e.to_string()),
                ))
            }
        };
        Ok(url.host().unwrap().to_string())
    } else {
        Err(IoError::new(ErrorKind::Other, "no context found"))
    }
}

fn pre_install_check() -> Result<(), CliError> {
    let helm_version = Command::new("helm")
        .arg("version")
        .arg("--short")
        .output()
        .map_err(|err| {
            CliError::Other(format!(
                "Helm package manager not found: {}",
                err.to_string()
            ))
        })?;
    let version_text = String::from_utf8(helm_version.stdout).unwrap();
    let version_text_trimmed = &version_text[1..].trim();

    const DEFAULT_HELM_VERSION: &str = "3.2.0";

    if Version::parse(&version_text_trimmed) < Version::parse(DEFAULT_HELM_VERSION) {
        return Err(CliError::Other(format!(
            "Helm version {} is not compatible with fluvio platform, please install version >= {}",
            version_text_trimmed, DEFAULT_HELM_VERSION
        )));
    }

    const SYS_CHART_VERSION: &str = "";
    const SYS_CHART_NAME: &str = "fluvio-sys";

    let sys_charts = helm::installed_sys_charts(SYS_CHART_NAME);
    if sys_charts.len() == 1 {
        let installed_chart = sys_charts.first().unwrap();
        let installed_chart_version = installed_chart.app_version.clone();
        // checking version of chart found
        if Version::parse(&installed_chart_version) < Version::parse(SYS_CHART_VERSION) {
            return Err(CliError::Other(format!(
                "Fluvio system chart {} is not compatible with fluvio platform, please install version >= {}",
                installed_chart_version, SYS_CHART_VERSION
            )));
        }
    } else if sys_charts.is_empty() {
        return Err(CliError::Other(
            "Fluvio system chart is not installed, please install fluvio-sys first".to_string(),
        ));
    } else {
        return Err(CliError::Other(
            "Multiple fluvio system charts found".to_string(),
        ));
    }

    let k8_config = K8Config::load()?;

    match k8_config {
        K8Config::Pod(_) => {
            // ignore server check for pod
        }
        K8Config::KubeConfig(config) => {
            let server_host = match get_cluster_server_host(config) {
                Ok(server) => server,
                Err(e) => {
                    return Err(CliError::Other(format!(
                        "error fetching server from kube context {}",
                        e.to_string()
                    )))
                }
            };
            if !server_host.trim().is_empty() {
                if IpAddr::from_str(&server_host).is_ok() {
                    return Err(CliError::Other(
                        format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
                    ));
                };
            } else {
                return Err(CliError::Other(
                    "Cluster in kubectl context cannot have empty hostname".to_owned(),
                ));
            }
        }
    };

    Ok(())
}

pub async fn install_core(opt: InstallCommand) -> Result<(), CliError> {
    pre_install_check().map_err(|err| CliError::Other(err.to_string()))?;

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
            builder = builder.with_chart_location(chart_location);
        }
        // If we're in develop mode (but no explicit chart location), use hardcoded local path
        None if opt.develop => {
            builder = builder.with_chart_location("./k8-util/helm/fluvio-app");
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

pub fn install_sys(opt: InstallCommand) {
    helm::repo_add(opt.k8_config.chart_location.as_deref());
    helm::repo_update();

    let mut cmd = Command::new("helm");
    cmd.arg("install").arg("fluvio-sys");

    if opt.develop {
        cmd.arg(
            opt.k8_config
                .chart_location
                .as_deref()
                .unwrap_or("./k8-util/helm/fluvio-sys"),
        );
    } else {
        cmd.arg(
            opt.k8_config
                .chart_location
                .as_deref()
                .unwrap_or("fluvio/fluvio-sys"),
        );
    }

    cmd.arg("--set")
        .arg(format!("cloud={}", opt.k8_config.cloud))
        .inherit();
    println!("fluvio sys chart has been installed");
}
