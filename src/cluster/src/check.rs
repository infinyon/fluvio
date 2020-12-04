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
use serde_json::Error as JsonError;
use k8_config::{ConfigError as K8ConfigError, K8Config};
use url::{Url, ParseError};

use fluvio_helm::{HelmClient, HelmError};
use crate::{DEFAULT_NAMESPACE, DEFAULT_CHART_SYS_REPO, DEFAULT_CHART_APP_REPO, DEFAULT_HELM_VERSION};

const DUMMY_LB_SERVICE: &str = "fluvio-dummy-service";
const DELAY: u64 = 1000;
const MINIKUBE_USERNAME: &str = "minikube";
const KUBE_VERSION: &str = "1.7.0";
const RESOURCE_SERVICE: &str = "service";
const RESOURCE_CRD: &str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &str = "secret";

pub type CheckResult = std::result::Result<CheckStatus, CheckError>;

/// A collection of the successes, failures, and errors of running checks
#[derive(Debug)]
pub struct CheckResults(pub(crate) Vec<CheckResult>);

impl From<Vec<CheckResult>> for CheckResults {
    fn from(it: Vec<CheckResult>) -> Self {
        Self(it)
    }
}

impl CheckResults {
    pub fn into_statuses(self) -> CheckStatuses {
        let statuses: Vec<_> = self
            .0
            .into_iter()
            .filter_map(|it| match it {
                Ok(status) => Some(status),
                Err(_) => None,
            })
            .collect();
        CheckStatuses::from(statuses)
    }
}

/// An error occurred during the checking process
///
/// All of these variants indicate that a check was unable to complete.
/// This is distinct from a "check failure" in which the check was able
/// to complete and the verdict is that the system is not prepared to
/// start a Fluvio cluster.
#[derive(thiserror::Error, Debug)]
pub enum CheckError {
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

    /// Error while fetching create permissions for a resource
    #[error("Unable to fetch permissions")]
    FetchPermissionError,

    /// Unable to parse kubectl version
    #[error("Unable to parse kubectl version from JSON")]
    KubectlVersionJsonError(JsonError),

    /// Could not create dummy service
    #[error("Could not create service")]
    ServiceCreateError,

    /// Could not delete dummy service
    #[error("Could not delete service")]
    ServiceDeleteError,
}

/// A collection of the successes, failures, and errors of running checks
#[derive(Debug)]
pub struct CheckStatuses(pub(crate) Vec<CheckStatus>);

impl From<Vec<CheckStatus>> for CheckStatuses {
    fn from(it: Vec<CheckStatus>) -> Self {
        Self(it)
    }
}

/// When a check completes without error, it either passes or fails
#[derive(Debug)]
pub enum CheckStatus {
    /// This check has passed and has the given success message
    Pass(CheckSucceeded),
    /// This check has failed and has the given failure reason
    Fail(CheckFailed),
}

impl CheckStatus {
    /// Creates a passing check status with a success message
    pub(crate) fn pass<S: Into<String>>(msg: S) -> Self {
        Self::Pass(msg.into())
    }

    /// Creates a failed check status with the given failure
    pub(crate) fn fail<F: Into<CheckFailed>>(fail: F) -> Self {
        Self::Fail(fail.into())
    }
}

/// A successful check yields a success message
pub type CheckSucceeded = String;

/// A description of a failed check
#[derive(thiserror::Error, Debug)]
pub enum CheckFailed {
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

/// A type of check failure which may be automatically recovered from
#[derive(thiserror::Error, Debug)]
pub enum RecoverableCheck {
    /// The fluvio-sys chart is not installed
    #[error("Missing Fluvio system charts.")]
    MissingSystemChart,

    /// Minikube tunnel not found, this error is used in case of linux where we can try to bring tunnel up
    #[error("Minikube tunnel not found")]
    MinikubeTunnelNotFoundRetry,
}

/// A type of check failure which is not recoverable
#[derive(thiserror::Error, Debug)]
pub enum UnrecoverableCheck {
    /// We failed to recover from a potentially-recoverable check failure
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

    /// There is no current Kubernetes context
    #[error("There is no active Kubernetes context")]
    NoActiveKubernetesContext,

    /// Unable to connect to the active context
    #[error("Failed to connect to Kubernetes via the active context")]
    CannotConnectToKubernetes,

    /// There are multiple fluvio-sys's installed
    #[error("Cannot have multiple versions of fluvio-sys installed")]
    MultipleSystemCharts,

    /// The current kubernetes cluster must have a server hostname
    #[error("Missing Kubernetes server host")]
    MissingKubernetesServerHost,

    /// There is no load balancer service is not available
    #[error("Load balancer service is not available")]
    LoadBalancerServiceNotAvailable,

    /// Minikube tunnel not found, this error is used in case of macos we don't try to get tunnel up as it needs elevated context
    #[error(
        "Minikube tunnel may not be running
    Run `sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &`"
    )]
    MinikubeTunnelNotFound,

    /// Default unhandled K8 client error
    #[error("Unhandled K8 client error")]
    UnhandledK8ClientError,
}

#[async_trait]
pub(crate) trait InstallCheck: Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> CheckResult;
}

pub(crate) struct LoadableConfig;

#[async_trait]
impl InstallCheck for LoadableConfig {
    async fn perform_check(&self) -> CheckResult {
        check_cluster_connection()
    }
}

pub(crate) struct K8Version;

#[async_trait]
impl InstallCheck for K8Version {
    async fn perform_check(&self) -> CheckResult {
        k8_version_check()
    }
}

pub(crate) struct HelmVersion;

#[async_trait]
impl InstallCheck for HelmVersion {
    async fn perform_check(&self) -> CheckResult {
        let helm_client = HelmClient::new().map_err(CheckError::HelmError)?;
        check_helm_version(&helm_client, DEFAULT_HELM_VERSION)
    }
}

pub(crate) struct SysChart;

#[async_trait]
impl InstallCheck for SysChart {
    async fn perform_check(&self) -> CheckResult {
        let helm_client = HelmClient::new().map_err(CheckError::HelmError)?;
        check_system_chart(&helm_client, DEFAULT_CHART_SYS_REPO)
    }
}

pub(crate) struct AlreadyInstalled;

#[async_trait]
impl InstallCheck for AlreadyInstalled {
    async fn perform_check(&self) -> CheckResult {
        let helm_client = HelmClient::new().map_err(CheckError::HelmError)?;
        check_already_installed(&helm_client, DEFAULT_CHART_APP_REPO)
    }
}

struct CreateServicePermission;

#[async_trait]
impl InstallCheck for CreateServicePermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_SERVICE)
    }
}

struct CreateCrdPermission;

#[async_trait]
impl InstallCheck for CreateCrdPermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_CRD)
    }
}

struct CreateServiceAccountPermission;

#[async_trait]
impl InstallCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_SERVICE_ACCOUNT)
    }
}

pub(crate) struct LoadBalancer;

#[async_trait]
impl InstallCheck for LoadBalancer {
    async fn perform_check(&self) -> CheckResult {
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
pub(crate) fn check_helm_version(helm: &HelmClient, required: &str) -> CheckResult {
    let helm_version = helm.get_helm_version().map_err(CheckError::HelmError)?;
    if Version::parse(&helm_version) < Version::parse(required) {
        return Ok(CheckStatus::fail(
            UnrecoverableCheck::IncompatibleHelmVersion {
                installed: helm_version,
                required: required.to_string(),
            },
        ));
    }
    Ok(CheckStatus::pass("Supported helm version is installed"))
}

/// Check that the system chart is installed
pub(crate) fn check_system_chart(helm: &HelmClient, sys_repo: &str) -> CheckResult {
    // check installed system chart version
    let sys_charts = helm
        .get_installed_chart_by_name(sys_repo)
        .map_err(CheckError::HelmError)?;
    if sys_charts.is_empty() {
        return Ok(CheckStatus::fail(RecoverableCheck::MissingSystemChart));
    } else if sys_charts.len() > 1 {
        return Ok(CheckStatus::fail(UnrecoverableCheck::MultipleSystemCharts));
    }
    Ok(CheckStatus::pass("Fluvio system charts are installed"))
}

/// Checks that Fluvio is not already installed
pub(crate) fn check_already_installed(helm: &HelmClient, app_repo: &str) -> CheckResult {
    let app_charts = helm
        .get_installed_chart_by_name(app_repo)
        .map_err(CheckError::HelmError)?;
    if !app_charts.is_empty() {
        return Ok(CheckStatus::fail(CheckFailed::AlreadyInstalled));
    }
    Ok(CheckStatus::pass("Previous fluvio installation not found"))
}

/// Check if load balancer is up
pub(crate) async fn check_load_balancer_status() -> CheckResult {
    let config = K8Config::load().map_err(CheckError::K8ConfigError)?;
    let context = match config {
        K8Config::Pod(_) => return Ok(CheckStatus::pass("Pod config found, ignoring the check")),
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = match context.config.current_context() {
        Some(context) => context,
        None => {
            return Ok(CheckStatus::fail(
                UnrecoverableCheck::NoActiveKubernetesContext,
            ));
        }
    };

    let username = &cluster_context.context.user;

    // create dummy service
    create_dummy_service()?;
    if wait_for_service_exist(DEFAULT_NAMESPACE).await?.is_some() {
        // IP found, everything good
        delete_service()?;
    } else {
        delete_service()?;
        if username == MINIKUBE_USERNAME {
            // In case of macos we need to run tunnel with elevated context of sudo
            // hence handle both separately
            return Ok(CheckStatus::fail(get_tunnel_error()));
        }
        return Ok(CheckStatus::fail(
            UnrecoverableCheck::LoadBalancerServiceNotAvailable,
        ));
    }

    Ok(CheckStatus::pass("Load balancer is up"))
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
            Err(K8ClientError::Client(status)) if status == StatusCode::NOT_FOUND => {
                sleep(Duration::from_millis(DELAY)).await;
            }
            Err(e) => return Err(CheckError::K8ClientError(e)),
        };
    }

    Ok(None)
}

#[cfg(target_os = "macos")]
fn get_tunnel_error() -> CheckFailed {
    UnrecoverableCheck::MinikubeTunnelNotFound.into()
}

#[cfg(not(target_os = "macos"))]
fn get_tunnel_error() -> CheckFailed {
    RecoverableCheck::MinikubeTunnelNotFoundRetry.into()
}

/// Checks that we can connect to Kubernetes via the active context
fn check_cluster_connection() -> CheckResult {
    let config = match K8Config::load() {
        Ok(config) => config,
        Err(K8ConfigError::NoCurrentContext) => {
            return Ok(CheckStatus::fail(
                UnrecoverableCheck::NoActiveKubernetesContext,
            ));
        }
        Err(other) => return Err(CheckError::K8ConfigError(other)),
    };

    let context = match config {
        K8Config::Pod(_) => return Ok(CheckStatus::pass("Pod config found, ignoring the check")),
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = match context.config.current_cluster() {
        Some(context) => context,
        None => {
            return Ok(CheckStatus::fail(
                UnrecoverableCheck::NoActiveKubernetesContext,
            ));
        }
    };

    let server_url = &cluster_context.cluster.server;

    // Check that the server URL has a hostname, not just an IP
    let host_present = Url::parse(server_url)
        .ok()
        .and_then(|it| it.host().map(|host| host.to_string()))
        .map(|it| !it.is_empty())
        .unwrap_or(false);

    if !host_present {
        return Ok(CheckStatus::fail(
            UnrecoverableCheck::MissingKubernetesServerHost,
        ));
    }

    Ok(CheckStatus::pass("Kubernetes config is loadable"))
}

// Check if required kubectl version is installed
fn k8_version_check() -> CheckResult {
    let kube_version = Command::new("kubectl")
        .arg("version")
        .arg("-o=json")
        .output()
        .map_err(CheckError::KubectlNotFoundError)?;
    let version_text = String::from_utf8(kube_version.stdout).unwrap();

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ComponentVersion {
        git_version: String,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct KubernetesVersion {
        client_version: ComponentVersion,
        server_version: Option<ComponentVersion>,
    }

    let kube_versions: KubernetesVersion =
        serde_json::from_str(&version_text).map_err(CheckError::KubectlVersionJsonError)?;

    let server_version = match kube_versions.server_version {
        Some(version) => version.git_version,
        None => {
            return Ok(CheckStatus::fail(
                UnrecoverableCheck::CannotConnectToKubernetes,
            ))
        }
    };

    // Trim off the `v` in v0.1.2 to get just "0.1.2"
    let server_version = &server_version[1..];
    if Version::parse(&server_version) < Version::parse(KUBE_VERSION) {
        return Ok(CheckStatus::fail(
            UnrecoverableCheck::IncompatibleKubectlVersion {
                installed: server_version.to_string(),
                required: KUBE_VERSION.to_string(),
            },
        ));
    }
    Ok(CheckStatus::pass(
        "Supported kubernetes version is installed",
    ))
}

fn check_permission(resource: &str) -> CheckResult {
    let res = check_create_permission(resource)?;
    if !res {
        return Ok(CheckStatus::fail(UnrecoverableCheck::PermissionError {
            resource: resource.to_string(),
        }));
    }
    Ok(CheckStatus::pass(format!("Can create {}", resource)))
}

fn check_create_permission(resource: &str) -> Result<bool, CheckError> {
    let check_command = Command::new("kubectl")
        .arg("auth")
        .arg("can-i")
        .arg("create")
        .arg(resource)
        .output()
        .map_err(CheckError::KubectlNotFoundError)?;
    let res =
        String::from_utf8(check_command.stdout).map_err(|_| CheckError::FetchPermissionError)?;
    Ok(res.trim() == "yes")
}
