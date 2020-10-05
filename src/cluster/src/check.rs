use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use std::process::{Command};

use thiserror::Error;
use k8_client::load_and_share;
use k8_obj_metadata::InputObjectMeta;
use k8_obj_core::service::ServiceSpec;
use k8_client::ClientError as K8ClientError;
use fluvio_future::timer::sleep;
use semver::Version;
use k8_config::{ConfigError as K8ConfigError, K8Config};
use url::{Url, ParseError};
use crate::helm::{HelmError, HelmClient};
use crate::install::{DEFAULT_NAMESPACE};

const DUMMY_LB_SERVICE: &str = "fluvio-dummy-service";

#[derive(Error, Debug)]
pub enum CheckError {
    /// The fluvio-sys chart is not installed
    #[error("The fluvio-sys chart is not installed")]
    MissingSystemChart,

    /// Fluvio is already correctly installed
    #[error("The fluvio-app chart is already installed")]
    AlreadyInstalled,

    /// Need to update minikube context
    #[error("The minikube context is not active or does not match your minikube ip")]
    InvalidMinikubeContext,

    /// There is no current kubernetest context
    #[error("There is no active Kubernetes context")]
    NoActiveKubernetesContext,

    /// Failed to parse kubernetes cluster server URL
    #[error("Failed to parse server url from Kubernetes context")]
    BadKubernetesServerUrl {
        #[from]
        source: ParseError,
    },

    /// There are multiple fluvio-sys's installed
    #[error("Cannot have multiple versions of fluvio-sys installed")]
    MultipleSystemCharts,

    /// The current kubernetes cluster must have a server hostname
    #[error("Missing Kubernetes server host")]
    MissingKubernetesServerHost,

    /// The server address for the current cluster must be a hostname, not an IP
    #[error("Kubernetes server must be a hostname, not an IP address")]
    KubernetesServerIsIp,

    /// The installed version of helm is incompatible
    #[error("Must have helm version {required} or later. You have {installed}")]
    IncompatibleHelmVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },

    /// There was a problem with the helm client during pre-check
    #[error("Helm client error")]
    HelmError {
        #[from]
        source: HelmError,
    },

    #[error("Kubernetes config error")]
    K8ConfigError {
        #[from]
        source: K8ConfigError,
    },

    /// Could not connect to K8 client, external IP/hostname not found
    #[error("Kubernetes client error")]
    K8ClientError {
        #[from]
        source: K8ClientError,
    },

    /// There is no load balancer service is not available
    #[error("Load balancer service is not available")]
    LoadBalancerServiceNotAvailable,

    /// Could not create dummy service
    #[error("Could not create service")]
    ServiceCreateError,

    /// Could not delete dummy service
    #[error("Could not delete service")]
    ServiceDeleteError,

    /// Minikube tunnel not found, this error is used in case of macos we don't try to get tunnel up as it needs elevated context
    #[error(
        r#"Load balancer service not found. 
  Please make sure you have minikube tunnel up and running.
  Run `sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &`"#
    )]
    MinikubeTunnelNotFound,

    /// Minikube tunnel not found, this error is used in case of linux where we can try to bring tunnel up
    #[error("Minikube tunnel not found, retrying")]
    MinikubeTunnelNotFoundRetry,
}

/// Getting server hostname from K8 context
pub fn check_cluster_server_host() -> Result<(), CheckError> {
    let config = K8Config::load()?;
    let context = match config {
        K8Config::Pod(_) => return Ok(()),
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = context
        .config
        .current_cluster()
        .ok_or(CheckError::NoActiveKubernetesContext)?;
    let server_url = cluster_context.cluster.server.to_owned();
    let url =
        Url::parse(&server_url).map_err(|source| CheckError::BadKubernetesServerUrl { source })?;
    let host = url
        .host()
        .ok_or(CheckError::MissingKubernetesServerHost)?
        .to_string();
    if host.is_empty() {
        return Err(CheckError::MissingKubernetesServerHost);
    }
    if IpAddr::from_str(&host).is_ok() {
        return Err(CheckError::KubernetesServerIsIp);
    }

    Ok(())
}

/// Checks that the installed helm version is compatible with the installer requirements
pub fn check_helm_version(helm: &HelmClient, required: &str) -> Result<(), CheckError> {
    let helm_version = helm.get_helm_version()?;
    if Version::parse(&helm_version) < Version::parse(required) {
        return Err(CheckError::IncompatibleHelmVersion {
            installed: helm_version,
            required: required.to_string(),
        });
    }
    Ok(())
}

/// Check that the system chart is installed
pub fn check_system_chart(helm: &HelmClient, sys_repo: &str) -> Result<(), CheckError> {
    // check installed system chart version
    let sys_charts = helm.get_installed_chart_by_name(sys_repo)?;
    if sys_charts.is_empty() {
        return Err(CheckError::MissingSystemChart);
    } else if sys_charts.len() > 1 {
        return Err(CheckError::MultipleSystemCharts);
    }
    Ok(())
}

/// Checks that Fluvio is not already installed
pub fn check_already_installed(helm: &HelmClient, app_repo: &str) -> Result<(), CheckError> {
    let app_charts = helm.get_installed_chart_by_name(app_repo)?;
    if !app_charts.is_empty() {
        return Err(CheckError::AlreadyInstalled);
    }
    Ok(())
}

/// check if load balancer is up
pub async fn check_load_balancer_status() -> Result<(), CheckError> {
    let config = K8Config::load()?;
    let context = match config {
        K8Config::Pod(_) => return Ok(()),
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = context
        .config
        .current_context()
        .ok_or(CheckError::NoActiveKubernetesContext)?;
    let username = cluster_context.context.user.to_owned();

    // create dummy service
    create_dummy_service()?;
    if wait_for_service_exist(DEFAULT_NAMESPACE).await?.is_some() {
        // IP found, everything good
        delete_service()?;
    } else {
        delete_service()?;
        if username == "minikube" {
            // incase of macos we need to run tunnel with elevated context of sudo
            // hence handle both seperately
            return Err(get_tunnel_error());
        }
        return Err(CheckError::LoadBalancerServiceNotAvailable);
    }

    Ok(())
}

fn create_dummy_service() -> Result<(), CheckError> {
    Command::new("kubectl")
        .arg("create")
        .arg("service")
        .arg("loadbalancer")
        .arg(DUMMY_LB_SERVICE)
        .arg("--tcp=5678:8080")
        .output()
        .map_err(|_| CheckError::ServiceCreateError)?;

    Ok(())
}

fn delete_service() -> Result<(), CheckError> {
    Command::new("kubectl")
        .arg("delete")
        .arg("service")
        .arg(DUMMY_LB_SERVICE)
        .output()
        .map_err(|_| CheckError::ServiceDeleteError)?;
    Ok(())
}

async fn wait_for_service_exist(ns: &str) -> Result<Option<String>, CheckError> {
    use k8_client::metadata::MetadataClient;
    use k8_client::http::StatusCode;

    let client = load_and_share()?;

    let input = InputObjectMeta::named(DUMMY_LB_SERVICE, ns);

    for _ in 0..10u16 {
        match client.retrieve_item::<ServiceSpec, _>(&input).await {
            Ok(svc) => {
                // check if load balancer status exists
                if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                    return Ok(Some(addr.to_owned()));
                } else {
                    sleep(Duration::from_millis(3000)).await;
                }
            }
            Err(err) => match err {
                K8ClientError::Client(status) if status == StatusCode::NOT_FOUND => {
                    sleep(Duration::from_millis(3000)).await;
                }
                _ => panic!("error: {}", err),
            },
        };
    }

    Ok(None)
}

#[cfg(target_os = "macos")]
fn get_tunnel_error() -> CheckError {
    CheckError::MinikubeTunnelNotFound
}

#[cfg(not(target_os = "macos"))]
fn get_tunnel_error() -> CheckError {
    CheckError::MinikubeTunnelNotFoundRetry
}
