use std::io::Error as IoError;
use std::time::Duration;
use std::process::{Command};

use async_trait::async_trait;
use k8_client::load_and_share;
use k8_obj_metadata::InputObjectMeta;
use k8_obj_core::service::ServiceSpec;
use k8_client::ClientError as K8ClientError;
use fluvio_future::timer::sleep;
use semver::Version;
use serde_json::{Value};
use k8_config::{ConfigError as K8ConfigError, K8Config};
use url::{Url, ParseError};

use fluvio_helm::{HelmClient, HelmError};
use crate::{
    DEFAULT_NAMESPACE, DEFAULT_CHART_SYS_REPO, DEFAULT_CHART_APP_REPO, DEFAULT_HELM_VERSION,
};

const DUMMY_LB_SERVICE: &str = "fluvio-dummy-service";
const DELAY: u64 = 1000;
const MINIKUBE_USERNAME: &str = "minikube";
const KUBE_VERSION: &str = "1.7.0";
const RESOURCE_SERVICE: &str = "service";
const RESOURCE_CRD: &str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &str = "secret";

#[derive(Debug)]
pub struct CheckResults(pub(crate) Vec<Result<String, CheckError>>);

impl From<Vec<Result<String, CheckError>>> for CheckResults {
    fn from(it: Vec<Result<String, CheckError>>) -> Self {
        Self(it)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CheckError {
    /// A cluster pre-start check that is potentially auto-recoverable
    #[error(transparent)]
    AutoRecoverable(#[from] RecoverableCheck),
    /// A cluster pre-start check that is unrecoverable
    #[error(transparent)]
    Unrecoverable(#[from] UnrecoverableCheck),
    /// Indicates that a cluster is already started
    #[error("Fluvio cluster is already started")]
    AlreadyInstalled,
}

#[derive(thiserror::Error, Debug)]
pub enum RecoverableCheck {
    /// The fluvio-sys chart is not installed
    #[error("Missing Fluvio system charts.")]
    MissingSystemChart,

    /// Minikube tunnel not found, this error is used in case of linux where we can try to bring tunnel up
    #[error("Minikube tunnel not found")]
    MinikubeTunnelNotFoundRetry,
}

/// The type of error that can occur while running preinstall checks
#[derive(thiserror::Error, Debug)]
pub enum UnrecoverableCheck {
    /// There was a problem with the helm client during pre-check
    #[error("Helm client error")]
    HelmError(#[from] HelmError),

    /// There was a problem fetching kubernetes configuration
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] K8ConfigError),

    /// Could not connect to K8 client
    #[error("Kubernetes client error")]
    K8ClientError(#[from] K8ClientError),

    /// Failed to parse kubernetes cluster server URL
    #[error("Failed to parse server url from Kubernetes context")]
    BadKubernetesServerUrl(#[from] ParseError),

    /// Kubectl not found
    #[error("Kubectl not found")]
    KubectlNotFoundError(IoError),

    #[error("Failed to recover from auto-recoverable check")]
    FailedRecovery(RecoverableCheck),

    /// Check permissions to create k8 resources
    #[error("Permissions to create {resource} denied")]
    PermissionError {
        /// Name of the resource
        resource: String,
    },

    /// The installed version of helm is incompatible
    #[error("Must have helm version {required} or later. You have {installed}")]
    IncompatibleHelmVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },

    /// The installed version of Kubectl is incompatible
    #[error("Must have kubectl version {required} or later. You have {installed}")]
    IncompatibleKubectlVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },

    /// There is no current kubernetest context
    #[error("There is no active Kubernetes context")]
    NoActiveKubernetesContext,

    /// There are multiple fluvio-sys's installed
    #[error("Cannot have multiple versions of fluvio-sys installed")]
    MultipleSystemCharts,

    /// The current kubernetes cluster must have a server hostname
    #[error("Missing Kubernetes server host")]
    MissingKubernetesServerHost,

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
"Minikube tunnel may not be running
    Run `sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &`"
    )]
    MinikubeTunnelNotFound,

    /// Default unhandled K8 client error
    #[error("Unhandled K8 client error")]
    UnhandledK8ClientError,

    /// Unable to parse kubectl version
    #[error("Unable to parse kubectl version")]
    KubectlVersionError,

    /// Error while fetching create permissions for a resource
    #[error("Unable to fetch permissions")]
    FetchPermissionError,
}

#[async_trait]
pub(crate) trait InstallCheck: Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> Result<String, CheckError>;
}

pub(crate) struct LoadableConfig;

#[async_trait]
impl InstallCheck for LoadableConfig {
    async fn perform_check(&self) -> Result<String, CheckError> {
        check_cluster_server_host()
    }
}

pub(crate) struct K8Version;

#[async_trait]
impl InstallCheck for K8Version {
    async fn perform_check(&self) -> Result<String, CheckError> {
        k8_version_check()
    }
}

pub(crate) struct HelmVersion;

#[async_trait]
impl InstallCheck for HelmVersion {
    async fn perform_check(&self) -> Result<String, CheckError> {
        let helm_client = HelmClient::new()
            .map_err(UnrecoverableCheck::HelmError)?;
        check_helm_version(&helm_client, DEFAULT_HELM_VERSION)
    }
}

pub(crate) struct SysChart;

#[async_trait]
impl InstallCheck for SysChart {
    async fn perform_check(&self) -> Result<String, CheckError> {
        let helm_client = HelmClient::new()
            .map_err(UnrecoverableCheck::HelmError)?;
        check_system_chart(&helm_client, DEFAULT_CHART_SYS_REPO)
    }
}

pub(crate) struct AlreadyInstalled;

#[async_trait]
impl InstallCheck for AlreadyInstalled {
    async fn perform_check(&self) -> Result<String, CheckError> {
        let helm_client = HelmClient::new()
            .map_err(UnrecoverableCheck::HelmError)?;
        check_already_installed(&helm_client, DEFAULT_CHART_APP_REPO)
    }
}

struct CreateServicePermission;

#[async_trait]
impl InstallCheck for CreateServicePermission {
    async fn perform_check(&self) -> Result<String, CheckError> {
        check_permission(RESOURCE_SERVICE)
    }
}

struct CreateCrdPermission;

#[async_trait]
impl InstallCheck for CreateCrdPermission {
    async fn perform_check(&self) -> Result<String, CheckError> {
        check_permission(RESOURCE_CRD)
    }
}

struct CreateServiceAccountPermission;

#[async_trait]
impl InstallCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> Result<String, CheckError> {
        check_permission(RESOURCE_SERVICE_ACCOUNT)
    }
}

pub(crate) struct LoadBalancer;

#[async_trait]
impl InstallCheck for LoadBalancer {
    async fn perform_check(&self) -> Result<String, CheckError> {
        check_load_balancer_status().await
    }
}

/// Client to manage cluster check operations
#[derive(Debug)]
#[non_exhaustive]
pub struct ClusterChecker {}

impl ClusterChecker {
    /// Runs all the checks that are needed for fluvio cluster startup
    /// # Example
    /// ```no_run
    /// use fluvio_cluster::ClusterChecker;
    /// ClusterChecker::run_preflight_checks();
    /// ```
    pub async fn run_preflight_checks() -> CheckResults {
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

        // Collect results from running checks
        let mut results = vec![];

        for check in checks {
            let check_result = check.perform_check().await;
            results.push(check_result);
        }

        CheckResults::from(results)
    }
}

/// Checks that the installed helm version is compatible with the installer requirements
pub(crate) fn check_helm_version(
    helm: &HelmClient,
    required: &str,
) -> Result<String, CheckError> {
    let helm_version = helm.get_helm_version()
        .map_err(UnrecoverableCheck::HelmError)?;
    if Version::parse(&helm_version) < Version::parse(required) {
        return Err(UnrecoverableCheck::IncompatibleHelmVersion {
            installed: helm_version,
            required: required.to_string(),
        }.into());
    }
    Ok("Supported helm version is installed".to_string())
}

/// Check that the system chart is installed
pub(crate) fn check_system_chart(
    helm: &HelmClient,
    sys_repo: &str,
) -> Result<String, CheckError> {
    // check installed system chart version
    let sys_charts = helm.get_installed_chart_by_name(sys_repo)
        .map_err(UnrecoverableCheck::HelmError)?;
    if sys_charts.is_empty() {
        return Err(RecoverableCheck::MissingSystemChart.into());
    } else if sys_charts.len() > 1 {
        return Err(UnrecoverableCheck::MultipleSystemCharts.into());
    }
    Ok("Fluvio system charts are installed".to_string())
}

/// Checks that Fluvio is not already installed
pub(crate) fn check_already_installed(
    helm: &HelmClient,
    app_repo: &str,
) -> Result<String, CheckError> {
    let app_charts = helm.get_installed_chart_by_name(app_repo)
        .map_err(UnrecoverableCheck::HelmError)?;
    if !app_charts.is_empty() {
        return Err(CheckError::AlreadyInstalled);
    }
    Ok("Previous fluvio installation not found".to_string())
}

/// Check if load balancer is up
pub(crate) async fn check_load_balancer_status() -> Result<String, CheckError> {
    let config = K8Config::load()
        .map_err(UnrecoverableCheck::K8ConfigError)?;
    let context = match config {
        K8Config::Pod(_) => {
            return Ok("Pod config found, ignoring the check".to_string())
        }
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = context
        .config
        .current_context()
        .ok_or(UnrecoverableCheck::NoActiveKubernetesContext)?;
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
        return Err(UnrecoverableCheck::LoadBalancerServiceNotAvailable.into());
    }

    Ok("Load balancer is up".to_string())
}

fn create_dummy_service() -> Result<(), UnrecoverableCheck> {
    Command::new("kubectl")
        .arg("create")
        .arg("service")
        .arg("loadbalancer")
        .arg(DUMMY_LB_SERVICE)
        .arg("--tcp=5678:8080")
        .output()
        .map_err(|_| UnrecoverableCheck::ServiceCreateError)?;

    Ok(())
}

fn delete_service() -> Result<(), UnrecoverableCheck> {
    Command::new("kubectl")
        .arg("delete")
        .arg("service")
        .arg(DUMMY_LB_SERVICE)
        .output()
        .map_err(|_| UnrecoverableCheck::ServiceDeleteError)?;
    Ok(())
}

async fn wait_for_service_exist(ns: &str) -> Result<Option<String>, UnrecoverableCheck> {
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
                    return Err(UnrecoverableCheck::UnhandledK8ClientError);
                }
            },
        };
    }

    Ok(None)
}

#[cfg(target_os = "macos")]
fn get_tunnel_error() -> CheckError {
    UnrecoverableCheck::MinikubeTunnelNotFound.into()
}

#[cfg(not(target_os = "macos"))]
fn get_tunnel_error() -> CheckError {
    RecoverableCheck::MinikubeTunnelNotFoundRetry.into()
}

/// Getting server hostname from K8 context
fn check_cluster_server_host() -> Result<String, CheckError> {
    let config = K8Config::load()
        .map_err(UnrecoverableCheck::K8ConfigError)?;
    let context = match config {
        K8Config::Pod(_) => {
            return Ok("Pod config found, ignoring the check".to_string())
        }
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = context
        .config
        .current_cluster()
        .ok_or(UnrecoverableCheck::NoActiveKubernetesContext)?;
    let server_url = cluster_context.cluster.server.to_owned();
    let url = Url::parse(&server_url).map_err(UnrecoverableCheck::BadKubernetesServerUrl)?;
    let host = url
        .host()
        .ok_or(UnrecoverableCheck::MissingKubernetesServerHost)?
        .to_string();
    if host.is_empty() {
        return Err(UnrecoverableCheck::MissingKubernetesServerHost.into());
    }

    Ok("Kubernetes config is loadable".to_string())
}

// Check if required kubectl version is installed
fn k8_version_check() -> Result<String, CheckError> {
    let kube_version = Command::new("kubectl")
        .arg("version")
        .arg("-o=json")
        .output()
        .map_err(UnrecoverableCheck::KubectlNotFoundError)?;
    let version_text = String::from_utf8(kube_version.stdout).unwrap();

    let kube_version_json: Value =
        serde_json::from_str(&version_text).map_err(|_| UnrecoverableCheck::KubectlVersionError)?;

    let mut server_version = kube_version_json["serverVersion"]["gitVersion"].to_string();
    server_version.retain(|c| c != '"');
    let version_text_trimmed = &server_version[1..].trim();

    if Version::parse(&version_text_trimmed) < Version::parse(KUBE_VERSION) {
        return Err(UnrecoverableCheck::IncompatibleKubectlVersion {
            installed: version_text_trimmed.to_string(),
            required: KUBE_VERSION.to_string(),
        }.into());
    }
    Ok("Supported kubernetes version is installed".to_string())
}

fn check_permission(resource: &str) -> Result<String, CheckError> {
    let res = check_create_permission(resource)?;
    if !res {
        return Err(UnrecoverableCheck::PermissionError {
            resource: resource.to_string(),
        }.into());
    }
    Ok(format!("Can create {}", resource))
}

fn check_create_permission(resource: &str) -> Result<bool, UnrecoverableCheck> {
    let check_command = Command::new("kubectl")
        .arg("auth")
        .arg("can-i")
        .arg("create")
        .arg(resource)
        .output()
        .map_err(UnrecoverableCheck::KubectlNotFoundError)?;
    let res =
        String::from_utf8(check_command.stdout).map_err(|_| UnrecoverableCheck::FetchPermissionError)?;
    Ok(res.trim() == "yes")
}
