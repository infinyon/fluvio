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
pub(crate) enum StatusCheck {
    /// Everything seems to be working as expected, check passed
    Working(String),
    /// Check failed due to an error, there is no work around
    NotWorkingNoRemediation(CheckError),
    /// Check failed due to an error, there is a work around fix
    NotWorking(CheckError, String),
}

#[async_trait]
pub(crate) trait InstallCheck: Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> Result<StatusCheck, CheckError>;
}

pub(crate) struct LoadableConfig;

#[async_trait]
impl InstallCheck for LoadableConfig {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        check_cluster_server_host()
    }
}

pub(crate) struct K8Version;

#[async_trait]
impl InstallCheck for K8Version {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        k8_version_check()
    }
}

pub(crate) struct HelmVersion;

#[async_trait]
impl InstallCheck for HelmVersion {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        let helm_client = HelmClient::new()?;
        check_helm_version(&helm_client, DEFAULT_HELM_VERSION)
    }
}

pub(crate) struct SysChart;

#[async_trait]
impl InstallCheck for SysChart {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        let helm_client = HelmClient::new()?;
        check_system_chart(&helm_client, DEFAULT_CHART_SYS_REPO)
    }
}

pub(crate) struct AlreadyInstalled;

#[async_trait]
impl InstallCheck for AlreadyInstalled {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        let helm_client = HelmClient::new()?;
        check_already_installed(&helm_client, DEFAULT_CHART_APP_REPO)
    }
}

struct CreateServicePermission;

#[async_trait]
impl InstallCheck for CreateServicePermission {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        check_permission(RESOURCE_SERVICE)
    }
}

struct CreateCrdPermission;

#[async_trait]
impl InstallCheck for CreateCrdPermission {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        check_permission(RESOURCE_CRD)
    }
}

struct CreateServiceAccountPermission;

#[async_trait]
impl InstallCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        check_permission(RESOURCE_SERVICE_ACCOUNT)
    }
}

pub(crate) struct LoadBalancer;

#[async_trait]
impl InstallCheck for LoadBalancer {
    async fn perform_check(&self) -> Result<StatusCheck, CheckError> {
        check_load_balancer_status().await
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
                Ok(check) => match check {
                    StatusCheck::Working(success) => {
                        let msg = format!("ok: {}", success);
                        println!("✔️  {}", msg.green());
                    }
                    StatusCheck::NotWorkingNoRemediation(failure) => {
                        let msg = format!("failed: {}", failure);
                        println!("❌ {}", msg.red());
                        failures.push(failure);
                    }
                    StatusCheck::NotWorking(failure, message) => {
                        let msg = format!("failed: {}", failure);
                        println!("❌ {}", msg.red());
                        println!("      help: {}", message);
                        failures.push(failure);
                    }
                },
                Err(err) => {
                    let msg = format!("Unexpected error occurred: {}", err);
                    println!("{}", msg.red());
                    failures.push(err);
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
pub(crate) fn check_helm_version(
    helm: &HelmClient,
    required: &str,
) -> Result<StatusCheck, CheckError> {
    let helm_version = helm.get_helm_version()?;
    if Version::parse(&helm_version) < Version::parse(required) {
        return Ok(StatusCheck::NotWorking(
            CheckError::IncompatibleHelmVersion {
                installed: helm_version,
                required: required.to_string(),
            },
            format!(
                "Please upgrade your helm client to at least {}",
                KUBE_VERSION.to_string()
            ),
        ));
    }
    Ok(StatusCheck::Working(
        "Supported helm version is installed".to_string(),
    ))
}

/// Check that the system chart is installed
pub(crate) fn check_system_chart(
    helm: &HelmClient,
    sys_repo: &str,
) -> Result<StatusCheck, CheckError> {
    // check installed system chart version
    let sys_charts = helm.get_installed_chart_by_name(sys_repo)?;
    if sys_charts.is_empty() {
        return Ok(StatusCheck::NotWorking(
            CheckError::MissingSystemChart,
            "Fluvio system charts can be installed using commnd `fluvio cluster install --sys`"
                .to_string(),
        ));
    } else if sys_charts.len() > 1 {
        return Ok(StatusCheck::NotWorking(
            CheckError::MultipleSystemCharts,
            "Multiple fluvio System charts are installed. Please remove duplicate helm system chart(s)".to_string()));
    }
    Ok(StatusCheck::Working(
        "Fluvio system charts are installed".to_string(),
    ))
}

/// Checks that Fluvio is not already installed
pub(crate) fn check_already_installed(
    helm: &HelmClient,
    app_repo: &str,
) -> Result<StatusCheck, CheckError> {
    let app_charts = helm.get_installed_chart_by_name(app_repo)?;
    if !app_charts.is_empty() {
        return Ok(StatusCheck::NotWorking(
            CheckError::AlreadyInstalled,
            "Fluvio is already installed, Please uninstall before trying to install".to_string(),
        ));
    }
    Ok(StatusCheck::Working("".to_string()))
}

/// Check if load balancer is up
pub(crate) async fn check_load_balancer_status() -> Result<StatusCheck, CheckError> {
    let config = K8Config::load()?;
    let context = match config {
        K8Config::Pod(_) => {
            return Ok(StatusCheck::Working(
                "Pod config found, ignoring the check".to_string(),
            ))
        }
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
            let (err, message) = get_tunnel_error();
            return Ok(StatusCheck::NotWorking(err, message));
        }
        return Err(CheckError::LoadBalancerServiceNotAvailable);
    }

    Ok(StatusCheck::Working("Load balancer is up".to_string()))
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
    use k8_client::http::status::StatusCode;

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
fn get_tunnel_error() -> (CheckError, String) {
    (
        CheckError::MinikubeTunnelNotFound,
        "Please make sure you have minikube tunnel up and running.
    Run `sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &`"
            .to_string(),
    )
}

#[cfg(not(target_os = "macos"))]
fn get_tunnel_error() -> (CheckError, String) {
    (
        CheckError::MinikubeTunnelNotFoundRetry,
        "Please make sure you have minikube tunnel up and running.
    Run `nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &`"
            .to_string(),
    )
}

/// Getting server hostname from K8 context
fn check_cluster_server_host() -> Result<StatusCheck, CheckError> {
    let config = K8Config::load()?;
    let context = match config {
        K8Config::Pod(_) => {
            return Ok(StatusCheck::Working(
                "Pod config found, ignoring the check".to_string(),
            ))
        }
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
    
    Ok(StatusCheck::Working(
        "Kubernetes config is loadable".to_string()
    ))
}

// Check if required kubectl version is installed
fn k8_version_check() -> Result<StatusCheck, CheckError> {
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
        return Ok(StatusCheck::NotWorking(
            CheckError::IncompatibleKubectlVersion {
                installed: version_text_trimmed.to_string(),
                required: KUBE_VERSION.to_string(),
            },
            format!(
                "Please upgrade your Kubernetes clusters to at least {}",
                KUBE_VERSION.to_string()
            ),
        ));
    }
    Ok(StatusCheck::Working(
        "Supported kubernetes version is installed".to_string(),
    ))
}

fn check_permission(resource: &str) -> Result<StatusCheck, CheckError> {
    let res = check_create_permission(resource)?;
    if !res {
        return Ok(StatusCheck::NotWorkingNoRemediation(
            CheckError::PermissionError {
                resource: resource.to_string(),
            },
        ));
    }
    Ok(StatusCheck::Working(format!("Can create {}", resource)))
}

fn check_create_permission(resource: &str) -> Result<bool, CheckError> {
    let check_command = Command::new("kubectl")
        .arg("auth")
        .arg("can-i")
        .arg("create")
        .arg(resource)
        .output()?;
    let res =
        String::from_utf8(check_command.stdout).map_err(|_| CheckError::FetchPermissionError)?;
    Ok(res.trim() == "yes")
}
