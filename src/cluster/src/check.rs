use std::net::IpAddr;
use std::str::FromStr;
use std::io::Error as IoError;
use std::time::Duration;
use std::process::{Command};

use async_trait::async_trait;
use thiserror::Error;
use k8_client::load_and_share;
use k8_obj_metadata::InputObjectMeta;
use k8_obj_core::service::ServiceSpec;
use k8_client::ClientError as K8ClientError;
use fluvio_future::timer::sleep;
use colored::*;
use semver::Version;
use serde_json::{Value};
use k8_config::{ConfigError as K8ConfigError, K8Config};
use url::{Url, ParseError};
use crate::helm::{HelmError, HelmClient};
use crate::install::{
    DEFAULT_NAMESPACE, DEFAULT_CHART_SYS_REPO, DEFAULT_CHART_APP_REPO, DEFAULT_HELM_VERSION,
};

const DUMMY_LB_SERVICE: &str = "fluvio-dummy-service";
const DELAY: u64 = 1000;
const MINIKUBE_USERNAME: &str = "minikube";
const KUBE_VERSION: &str = "1.7.0";
const RESOURCE_SERVICE: &str = "service";
const RESOURCE_CRD: &str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &str = "secret";

/// The type of error that can occur while running preinstall checks
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
        /// source : ParseError
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
        /// helm client error
        source: HelmError,
    },

    /// There was a problem fetching kubernetes configuration
    #[error("Kubernetes config error")]
    K8ConfigError {
        /// source : K8ConfigError
        #[from]
        source: K8ConfigError,
    },

    /// Could not connect to K8 client, external IP/hostname not found
    #[error("Kubernetes client error")]
    K8ClientError {
        /// source : K8ClientError
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

    /// Default unhandled K8 client error
    #[error("Unhandled K8 client error")]
    UnhandledK8ClientError,

    /// Kubectl not found
    #[error("Kubectl not found")]
    KubectlNotFoundError {
        /// source : IoError
        #[from]
        source: IoError,
    },

    /// Unable to parse kubectl version
    #[error("Unable to parse kubectl version")]
    KubectlVersionError,

    /// The installed version of Kubectl is incompatible
    #[error("Must have kubectl version {required} or later. You have {installed}")]
    IncompatibleKubectlVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },

    /// Check permissions to create k8 resources
    #[error("Permissions to create {resource} denied")]
    PermissionError {
        /// Name of the resource
        resource: String,
    },

    /// Error while fetching create permissions for a resource
    #[error("Unable to fetch permissions")]
    FetchPermissionError,

    /// One or more pre flight checks have failed
    #[error("Some pre-install checks have failed.")]
    PreFlightCheckError,
}

/// Captures the status of the check
pub enum StatusCheck {
    /// Everything seems to be working as expected, check passed
    Working(String),
    /// Check failed due to an error, there is no work around
    NotWorkingNoRemediation(CheckError),
    /// Check failed due to an error, there is a work around fix
    NotWorking(CheckError),
}

#[async_trait]
pub trait InstallCheck: Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> StatusCheck;
}

struct LoadableConfig;

#[async_trait]
impl InstallCheck for LoadableConfig {
    async fn perform_check(&self) -> StatusCheck {
        match check_cluster_server_host() {
            Ok(_) => StatusCheck::Working(
                "Kubernetes config is loadable and cluster hostname is not an IP address"
                    .to_string(),
            ),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct K8Version;

#[async_trait]
impl InstallCheck for K8Version {
    async fn perform_check(&self) -> StatusCheck {
        match k8_version_check() {
            Ok(_) => StatusCheck::Working("Supported kubernetes version is installed".to_string()),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct HelmVersion;

#[async_trait]
impl InstallCheck for HelmVersion {
    async fn perform_check(&self) -> StatusCheck {
        let helm_client = match HelmClient::new() {
            Ok(client) => client,
            Err(err) => {
                return StatusCheck::NotWorkingNoRemediation(CheckError::HelmError { source: err });
            }
        };
        match check_helm_version(&helm_client, DEFAULT_HELM_VERSION) {
            Ok(_) => StatusCheck::Working("Supported helm version is installed".to_string()),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct SysChart;

#[async_trait]
impl InstallCheck for SysChart {
    async fn perform_check(&self) -> StatusCheck {
        let helm_client = match HelmClient::new() {
            Ok(client) => client,
            Err(err) => {
                return StatusCheck::NotWorkingNoRemediation(CheckError::HelmError { source: err });
            }
        };
        match check_system_chart(&helm_client, DEFAULT_CHART_SYS_REPO) {
            Ok(_) => StatusCheck::Working("Fluvio system charts are installed".to_string()),
            Err(err) => StatusCheck::NotWorking(err),
        }
    }
}

struct AlreadyInstalled;

#[async_trait]
impl InstallCheck for AlreadyInstalled {
    async fn perform_check(&self) -> StatusCheck {
        let helm_client = match HelmClient::new() {
            Ok(client) => client,
            Err(err) => {
                return StatusCheck::NotWorkingNoRemediation(CheckError::HelmError { source: err });
            }
        };
        match check_already_installed(&helm_client, DEFAULT_CHART_APP_REPO) {
            Ok(_) => StatusCheck::Working("".to_string()),
            Err(err) => StatusCheck::NotWorking(err),
        }
    }
}

struct CreateServicePermission;

#[async_trait]
impl InstallCheck for CreateServicePermission {
    async fn perform_check(&self) -> StatusCheck {
        match check_permission(RESOURCE_SERVICE) {
            Ok(_) => StatusCheck::Working("Can create a Service".to_string()),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct CreateCrdPermission;

#[async_trait]
impl InstallCheck for CreateCrdPermission {
    async fn perform_check(&self) -> StatusCheck {
        match check_permission(RESOURCE_CRD) {
            Ok(_) => StatusCheck::Working("Can create CustomResourceDefinitions".to_string()),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct CreateServiceAccountPermission;

#[async_trait]
impl InstallCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> StatusCheck {
        match check_permission(RESOURCE_SERVICE_ACCOUNT) {
            Ok(_) => StatusCheck::Working("Can create ServiceAccount".to_string()),
            Err(err) => StatusCheck::NotWorkingNoRemediation(err),
        }
    }
}

struct LoadBalancer;

#[async_trait]
impl InstallCheck for LoadBalancer {
    async fn perform_check(&self) -> StatusCheck {
        match check_load_balancer_status().await {
            Ok(_) => StatusCheck::Working("Load Balancer is up".to_string()),
            Err(err) => StatusCheck::NotWorking(err),
        }
    }
}

/// Client to manage cluster check operations
#[derive(Debug)]
#[non_exhaustive]
pub struct ClusterChecker {}

impl ClusterChecker {
    /// Runs all the checks that are needed for fluvio cluster installation
    /// # Example
    /// ```no_run
    /// use fluvio_cluster::ClusterChecker;
    /// ClusterChecker::run_preflight_checks();
    /// ```
    pub async fn run_preflight_checks() -> Result<(), CheckError> {
        // List of checks
        let checks: Vec<Box<dyn InstallCheck>> = vec![
            Box::new(LoadableConfig),
            Box::new(K8Version),
            Box::new(HelmVersion),
            Box::new(SysChart),
            Box::new(CreateServicePermission),
            Box::new(CreateCrdPermission),
            Box::new(CreateServiceAccountPermission),
            Box::new(LoadBalancer),
        ];

        // capture failures if any
        let mut failures = Vec::new();
        println!("\nRunning pre-install checks....\n");

        for check in checks {
            match check.perform_check().await {
                StatusCheck::Working(success) => {
                    let msg = format!("ok: {}", success);
                    println!("✔️  {}", msg.green());
                }
                StatusCheck::NotWorkingNoRemediation(failure)
                | StatusCheck::NotWorking(failure) => {
                    let msg = format!("failed: {}", failure);
                    println!("❌ {}", msg.red());
                    failures.push(failure);
                }
            }
        }

        // check if there are any failures and show final message
        if !failures.is_empty() {
            println!("\nSome pre-install checks have failed.\n");
            return Err(CheckError::PreFlightCheckError);
        } else {
            println!("\nAll checks passed!\n");
        }
        Ok(())
    }
}

/// Checks that the installed helm version is compatible with the installer requirements
pub(crate) fn check_helm_version(helm: &HelmClient, required: &str) -> Result<(), CheckError> {
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
pub(crate) fn check_system_chart(helm: &HelmClient, sys_repo: &str) -> Result<(), CheckError> {
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
pub(crate) fn check_already_installed(helm: &HelmClient, app_repo: &str) -> Result<(), CheckError> {
    let app_charts = helm.get_installed_chart_by_name(app_repo)?;
    if !app_charts.is_empty() {
        return Err(CheckError::AlreadyInstalled);
    }
    Ok(())
}

/// Check if load balancer is up
pub(crate) async fn check_load_balancer_status() -> Result<(), CheckError> {
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
        if username == MINIKUBE_USERNAME {
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
                    sleep(Duration::from_millis(DELAY)).await;
                }
            }
            Err(err) => match err {
                K8ClientError::Client(status) if status == StatusCode::NOT_FOUND => {
                    sleep(Duration::from_millis(DELAY)).await;
                }
                _ => {
                    return Err(CheckError::UnhandledK8ClientError);
                }
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

// Check if required kubectl version is installed
fn k8_version_check() -> Result<(), CheckError> {
    let kube_version = Command::new("kubectl")
        .arg("version")
        .arg("-o=json")
        .output()
        .map_err(|source| CheckError::KubectlNotFoundError { source })?;
    let version_text = String::from_utf8(kube_version.stdout).unwrap();

    let kube_version_json: Value =
        serde_json::from_str(&version_text).map_err(|_| CheckError::KubectlVersionError)?;

    let mut server_version = kube_version_json["serverVersion"]["gitVersion"].to_string();
    server_version.retain(|c| c != '"');
    let version_text_trimmed = &server_version[1..].trim();

    if Version::parse(&version_text_trimmed) < Version::parse(KUBE_VERSION) {
        return Err(CheckError::IncompatibleKubectlVersion {
            installed: version_text_trimmed.to_string(),
            required: KUBE_VERSION.to_string(),
        });
    }
    Ok(())
}

fn check_permission(resource: &str) -> Result<(), CheckError> {
    let res = check_create_permission(resource)?;
    if !res {
        return Err(CheckError::PermissionError {
            resource: resource.to_string(),
        });
    }
    Ok(())
}

fn check_create_permission(resource: &str) -> Result<bool, CheckError> {
    let check_command = Command::new("kubectl")
        .arg("auth")
        .arg("can-i")
        .arg("create")
        .arg(resource)
        .output();
    match check_command {
        Ok(out) => match String::from_utf8(out.stdout) {
            Ok(res) => Ok(res.trim() == "yes"),
            Err(_) => Err(CheckError::FetchPermissionError),
        },
        Err(_) => Err(CheckError::FetchPermissionError),
    }
}
