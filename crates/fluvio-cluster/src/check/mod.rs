use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::Error as IoError;
use std::fmt::Debug;
use std::process::{Command};
use std::time::Duration;

pub mod render;

use colored::Colorize;
use fluvio_future::timer::sleep;
use tracing::{error, debug};
use async_trait::async_trait;
use url::ParseError;
use semver::Version;
use serde_json::Error as JsonError;

use fluvio_helm::{HelmClient, HelmError};
use k8_config::{ConfigError as K8ConfigError, K8Config};
use k8_client::ClientError as K8ClientError;

use crate::charts::{DEFAULT_HELM_VERSION, APP_CHART_NAME};
use crate::render::ProgressRenderer;
use crate::charts::{ChartConfig, ChartInstaller, ChartInstallError, SYS_CHART_NAME};

const KUBE_VERSION: &str = "1.7.0";
const RESOURCE_SERVICE: &str = "service";
const RESOURCE_CRD: &str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &str = "secret";

/// The outcome of a check: it was either successfully performed, or it errored
///
/// Note that a check that comes back negative (a "failed" check) is still
/// captured by the `Ok` variant of a `CheckResult`, since the check completed
/// successfully. If the process of performing the check is what fails, we get
/// an `Err`.
pub type CheckResult = std::result::Result<CheckStatus, ClusterCheckError>;

/// A collection of the successes, failures, and errors of running checks
pub type CheckResults = Vec<CheckResult>;

/// An error occurred during the checking process
#[derive(thiserror::Error, Debug)]
pub enum ClusterCheckError {
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

    /// Unable to parse Error
    #[error("Could not parse Version")]
    VersionError(#[from] semver::Error),

    /// local fluvio exists
    #[error("Loocal Fluvio running")]
    LocalClusterExists,

    /// Other misc
    #[error("Other failure: {0}")]
    Other(String),
}

/// An error occurred during the checking process
#[derive(thiserror::Error, Debug)]
pub enum ClusterAutoFixError {
    /// There was a problem with the helm client during pre-check
    #[error("Helm client error")]
    Helm(#[from] HelmError),

    /// There was a problem fetching kubernetes configuration
    #[error("Kubernetes config error")]
    K8Config(#[from] K8ConfigError),

    /// Could not connect to K8 client
    #[error("Kubernetes client error")]
    K8Client(#[from] K8ClientError),

    #[error("Chart Install error")]
    ChartInstall(#[from] ChartInstallError),
}

/// Allows checks to suggest further action
pub trait CheckSuggestion {
    /// Returns `Some(suggestion)` if there is a suggestion
    /// to give, otherwise returns `None`.
    fn suggestion(&self) -> Option<String> {
        None
    }
}

/// A collection of the successes, failures, and errors of running checks
pub type CheckStatuses = Vec<CheckStatus>;

/// When a check completes without error, it either passes or fails
#[derive(Debug)]
pub enum CheckStatus {
    /// This check has passed and has the given success message
    Pass(CheckSucceeded),
    /// This check has failed but can be recovered
    AutoFixableError(Box<dyn ClusterAutoFix>),
    /// check that cannot be recovered
    Unrecoverable(UnrecoverableCheckStatus),
}

impl CheckStatus {
    /// Creates a passing check status with a success message
    pub(crate) fn pass<S: Into<String>>(msg: S) -> Self {
        Self::Pass(msg.into())
    }
}

/// A successful check yields a success message
pub type CheckSucceeded = String;

/// A type of check failure which may be automatically recovered from
#[derive(thiserror::Error, Debug)]
pub enum RecoverableCheck {
    /// The fluvio-sys chart is not installed
    #[error("Missing Fluvio system charts.")]
    MissingSystemChart,

    #[error("Fluvio system charts are not up to date.")]
    UpgradeSystemChart,
}

impl CheckSuggestion for RecoverableCheck {
    fn suggestion(&self) -> Option<String> {
        let suggestion = match self {
            Self::MissingSystemChart => "Run 'fluvio cluster start --sys'",
            Self::UpgradeSystemChart => "Run 'fluvio cluster start --sys'",
        };
        Some(suggestion.to_string())
    }
}

/// A type of check failure which is not recoverable
#[derive(thiserror::Error, Debug)]
pub enum UnrecoverableCheckStatus {
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

    #[error("Fluvio chart is already installed")]
    AlreadyInstalled,

    /// The current kubernetes cluster must have a server hostname
    #[error("Missing Kubernetes server host")]
    MissingKubernetesServerHost,

    /// There is no load balancer service is not available
    #[error("Load balancer service is not available")]
    LoadBalancerServiceNotAvailable,

    /// No Helm client
    #[error("No Helm client: {0}")]
    NoHelmClient(String),

    /// Default unhandled K8 client error
    #[error("Unhandled K8 client error: {0}")]
    UnhandledK8ClientError(String),

    #[error("Local Fluvio component still exists")]
    ExistingLocalCluster,

    /// Other misc
    #[error("Other failure: {0}")]
    Other(String),
}

impl CheckSuggestion for UnrecoverableCheckStatus {
    fn suggestion(&self) -> Option<String> {
        None
    }
}

/// Fluvio Cluster component
#[derive(Debug, Hash, PartialEq, Eq)]
pub enum FluvioClusterComponent {
    Helm,
    Kubernetes,
    K8Version,
    SysChart,
}

#[async_trait]
pub trait ClusterCheck: Debug + 'static + Send + Sync {
    /// Returns label that can be used
    fn label(&self) -> &str;

    /// can register as component that other checker can depend on
    fn component(&self) -> Option<FluvioClusterComponent> {
        None
    }

    /// list of components that must be installed before checking
    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![]
    }

    /// perform check, if successful return success message, if fail, return
    async fn perform_check(&self, pb: &ProgressRenderer) -> Result<CheckStatus, ClusterCheckError>;
}

#[async_trait]
pub trait ClusterAutoFix: Debug + 'static + Send + Sync {
    /// Attempt to fix a recoverable error. return string
    async fn attempt_fix(&self, render: &ProgressRenderer) -> Result<String, ClusterAutoFixError>;
}

/// Check for loading
#[derive(Debug)]
pub(crate) struct ActiveKubernetesCluster;

#[async_trait]
impl ClusterCheck for ActiveKubernetesCluster {
    /// Checks that we can connect to Kubernetes via the active context
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        pb.set_message("Checking for Active Kubernetes config");
        let config = match K8Config::load() {
            Ok(config) => config,
            Err(K8ConfigError::NoCurrentContext) => {
                return Ok(CheckStatus::Unrecoverable(
                    UnrecoverableCheckStatus::NoActiveKubernetesContext,
                ))
            }

            Err(err) => {
                return Ok(CheckStatus::Unrecoverable(
                    UnrecoverableCheckStatus::UnhandledK8ClientError(format!(
                        "K8 Error: {:#?}",
                        err
                    )),
                ))
            }
        };

        let context = match config {
            K8Config::Pod(_) => {
                return Ok(CheckStatus::Unrecoverable(UnrecoverableCheckStatus::Other(
                    "Pod config found".to_owned(),
                )))
            }
            K8Config::KubeConfig(context) => context,
        };

        match context.config.current_cluster() {
            Some(_ctx) => Ok(CheckStatus::pass("Active Kubernetes context found")),
            None => Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::NoActiveKubernetesContext,
            )),
        }
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![]
    }

    fn component(&self) -> Option<FluvioClusterComponent> {
        Some(FluvioClusterComponent::Kubernetes)
    }

    fn label(&self) -> &str {
        "Kubernetes config"
    }
}

#[derive(Debug)]
pub(crate) struct K8Version;

#[async_trait]
impl ClusterCheck for K8Version {
    /// Check if required kubectl version is installed
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        pb.set_message("Checking Kubernetes Version");
        let kube_version = Command::new("kubectl")
            .arg("version")
            .arg("-o=json")
            .output()
            .map_err(ClusterCheckError::KubectlNotFoundError)?;

        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ComponentVersion {
            git_version: String,
        }

        #[derive(Debug, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct KubernetesVersion {
            #[allow(dead_code)]
            client_version: ComponentVersion,
            server_version: Option<ComponentVersion>,
        }

        let kube_versions: KubernetesVersion = serde_json::from_slice(&kube_version.stdout)
            .map_err(ClusterCheckError::KubectlVersionJsonError)?;

        let server_version = match kube_versions.server_version {
            Some(version) => version.git_version,
            None => {
                return Ok(CheckStatus::Unrecoverable(
                    UnrecoverableCheckStatus::CannotConnectToKubernetes,
                ))
            }
        };

        // Trim off the `v` in v0.1.2 to get just "0.1.2"
        let server_version = &server_version[1..];
        if Version::parse(server_version)? < Version::parse(KUBE_VERSION)? {
            Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::IncompatibleKubectlVersion {
                    installed: server_version.to_string(),
                    required: KUBE_VERSION.to_string(),
                },
            ))
        } else {
            Ok(CheckStatus::pass(format!(
                "Kubernetes server version: {} is supported",
                server_version
            )))
        }
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![FluvioClusterComponent::Kubernetes]
    }

    fn component(&self) -> Option<FluvioClusterComponent> {
        Some(FluvioClusterComponent::K8Version)
    }

    fn label(&self) -> &str {
        "Kubernetes version"
    }
}

#[derive(Debug)]
pub(crate) struct HelmVersion;

#[async_trait]
impl ClusterCheck for HelmVersion {
    /// Checks that the installed helm version is compatible with the installer requirements
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        sleep(Duration::from_millis(5000)).await;
        let helm = match HelmClient::new() {
            Ok(client) => client,
            Err(err) => {
                return Ok(CheckStatus::Unrecoverable(
                    UnrecoverableCheckStatus::NoHelmClient(format!(
                        "Unable to find helm: {:#?}",
                        err
                    )),
                ))
            }
        };

        let helm_version = helm
            .get_helm_version()
            .map_err(ClusterCheckError::HelmError)?;
        let required = DEFAULT_HELM_VERSION;
        if Version::parse(&helm_version)? < Version::parse(required)? {
            return Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::IncompatibleHelmVersion {
                    installed: helm_version,
                    required: required.to_string(),
                },
            ));
        }
        Ok(CheckStatus::pass(format!(
            "Supported helm version: {} is installed",
            helm_version
        )))
    }

    fn component(&self) -> Option<FluvioClusterComponent> {
        Some(FluvioClusterComponent::Helm)
    }

    fn label(&self) -> &str {
        "Helm"
    }
}

#[derive(Debug)]
pub(crate) struct SysChartCheck {
    config: ChartConfig,
    platform_version: Version,
}

impl SysChartCheck {
    pub(crate) fn new(config: ChartConfig, platform_version: Version) -> Self {
        Self {
            config,
            platform_version,
        }
    }
}

#[async_trait]
impl ClusterCheck for SysChartCheck {
    /// Check that the system chart is installed
    /// This uses whatever namespace it is being called
    async fn perform_check(&self, _pb: &ProgressRenderer) -> CheckResult {
        debug!("performing sys chart check");

        let helm = HelmClient::new()?;
        // check installed system chart version
        let sys_charts = helm
            .get_installed_chart_by_name(SYS_CHART_NAME, None)
            .map_err(ClusterCheckError::HelmError)?;
        debug!(charts = sys_charts.len(), "sys charts count");
        if sys_charts.is_empty() {
            Ok(CheckStatus::AutoFixableError(Box::new(InstallSysChart(
                self.config.clone(),
            ))))
        } else if sys_charts.len() > 1 {
            Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::MultipleSystemCharts,
            ))
        } else {
            let install_chart = sys_charts.get(0).unwrap();
            debug!(app_version = %install_chart.app_version,"Sys Chart Version");
            let sys_platform_version = Version::parse(&install_chart.app_version)?;
            if sys_platform_version == self.platform_version {
                Ok(CheckStatus::pass("Fluvio system charts are installed"))
            } else {
                Ok(CheckStatus::AutoFixableError(Box::new(UpgradeSysChart {
                    config: self.config.clone(),
                    platform_version: self.platform_version.clone(),
                })))
            }
        }
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![
            FluvioClusterComponent::Helm,
            FluvioClusterComponent::Kubernetes,
        ]
    }

    fn component(&self) -> Option<FluvioClusterComponent> {
        Some(FluvioClusterComponent::SysChart)
    }

    fn label(&self) -> &str {
        "Fluvio Sys Chart"
    }
}

#[derive(Debug)]
pub(crate) struct InstallSysChart(ChartConfig);

#[async_trait]
impl ClusterAutoFix for InstallSysChart {
    async fn attempt_fix(&self, render: &ProgressRenderer) -> Result<String, ClusterAutoFixError> {
        debug!(
            "Fixing by installing Fluvio sys chart with config: {:#?}",
            &self.0
        );
        render.set_message("Installing Fluvio system charts...");
        let sys_installer = ChartInstaller::from_config(self.0.clone())?;
        sys_installer.install()?;

        Ok("Fluvio Sys chart is installed".to_owned())
    }
}

#[derive(Debug)]
pub(crate) struct UpgradeSysChart {
    config: ChartConfig,
    platform_version: Version,
}

#[async_trait]
impl ClusterAutoFix for UpgradeSysChart {
    async fn attempt_fix(&self, render: &ProgressRenderer) -> Result<String, ClusterAutoFixError> {
        debug!(
            "Fixing by updating Fluvio sys chart with config: {:#?}",
            &self.config
        );

        let sys_installer = ChartInstaller::from_config(self.config.clone())?;
        render.println(format!(
            "Upgrading Fluvio system charts to: {}",
            self.platform_version
        ));
        sys_installer.upgrade()?;

        Ok(format!(
            "Fluvio sys chart is upgraded to: {}",
            self.platform_version
        ))
    }
}

#[derive(Debug)]
pub(crate) struct AlreadyInstalled;

#[async_trait]
impl ClusterCheck for AlreadyInstalled {
    /// Checks that Fluvio is not already installed
    async fn perform_check(&self, _pb: &ProgressRenderer) -> CheckResult {
        let helm = HelmClient::new()?;
        let app_charts = helm.get_installed_chart_by_name(APP_CHART_NAME, None)?;
        if !app_charts.is_empty() {
            return Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::AlreadyInstalled,
            ));
        }
        Ok(CheckStatus::pass("Previous fluvio installation not found"))
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![
            FluvioClusterComponent::Helm,
            FluvioClusterComponent::Kubernetes,
        ]
    }

    fn label(&self) -> &str {
        "Fluvio installation"
    }
}

#[derive(Debug)]
struct CreateServicePermission;

#[async_trait]
impl ClusterCheck for CreateServicePermission {
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        check_permission(RESOURCE_SERVICE, pb)
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![FluvioClusterComponent::Kubernetes]
    }

    fn label(&self) -> &str {
        "Kubernetes Service Permission"
    }
}

#[derive(Debug)]
struct CreateCrdPermission;

#[async_trait]
impl ClusterCheck for CreateCrdPermission {
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        check_permission(RESOURCE_CRD, pb)
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![FluvioClusterComponent::Kubernetes]
    }

    fn label(&self) -> &str {
        "Kubernetes Crd Permission"
    }
}

#[derive(Debug)]
struct CreateServiceAccountPermission;

#[async_trait]
impl ClusterCheck for CreateServiceAccountPermission {
    async fn perform_check(&self, pb: &ProgressRenderer) -> CheckResult {
        check_permission(RESOURCE_SERVICE_ACCOUNT, pb)
    }

    fn required_components(&self) -> Vec<FluvioClusterComponent> {
        vec![FluvioClusterComponent::Kubernetes]
    }

    fn label(&self) -> &str {
        "Kubernetes Service Account Permission"
    }
}

/// check if local cluster is running
#[derive(Debug)]
struct LocalClusterCheck;

#[async_trait]
impl ClusterCheck for LocalClusterCheck {
    async fn perform_check(&self, _pb: &ProgressRenderer) -> CheckResult {
        println!("performing local cluster check");
        match Command::new("pgrep").arg("fluvio").output() {
            Ok(_) => Ok(CheckStatus::pass("No Local cluster exists")),
            Err(_err) => Ok(CheckStatus::Unrecoverable(
                UnrecoverableCheckStatus::ExistingLocalCluster,
            )),
        }
    }

    fn label(&self) -> &str {
        "Fluvio Local Installation"
    }
}

/// Manages all cluster check operations
///
/// A `ClusterChecker` can be configured with different sets of checks to run.
/// It can wait for all checks to run sequentially using [`run_wait`], or spawn a
/// task and receive progress updates about checks using [`run_with_progress`].
///
/// [`run_wait`]: ClusterChecker::run_wait
/// [`run_with_progress`]: ClusterChecker::run_with_progress
#[derive(Debug)]
#[non_exhaustive]
pub struct ClusterChecker {
    checks: Vec<Box<dyn ClusterCheck>>,
}

impl ClusterChecker {
    /// Creates an empty checker with no checks to be run.
    ///
    /// Be sure to use methods like [`with_check`] to add checks before
    /// calling one of the `run` methods or they will do nothing.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::ClusterChecker;
    /// let checker: ClusterChecker = ClusterChecker::empty();
    /// ```
    ///
    /// [`with_check`]: ClusterChecker::with_check
    pub fn empty() -> Self {
        ClusterChecker { checks: vec![] }
    }

    /// Adds a check to this `ClusterChecker`
    pub fn with_check<C: ClusterCheck, B: Into<Box<C>>>(mut self, check: B) -> Self {
        self.checks.push(check.into());
        self
    }

    /// Adds all preflight checks to this checker.
    ///
    /// Note that no checks are run until one of the `run` methods are invoked.
    ///
    /// - [`run_wait`](ClusterChecker::run_wait)
    /// - [`run_wait_and_fix`](ClusterChecker::run_wait_and_fix)
    /// - [`run_with_progress`](ClusterChecker::run_with_progress)
    /// - [`run_and_fix_with_progress`](ClusterChecker::run_and_fix_with_progress)
    pub fn with_preflight_checks(mut self) -> Self {
        let checks: Vec<Box<(dyn ClusterCheck)>> = vec![
            Box::new(ActiveKubernetesCluster),
            Box::new(K8Version),
            Box::new(HelmVersion),
            Box::new(CreateServicePermission),
            Box::new(CreateCrdPermission),
            Box::new(CreateServiceAccountPermission),
        ];
        self.checks.extend(checks);
        self
    }

    /// Adds all checks required for starting a cluster on minikube.
    ///
    /// Note that no checks are run until one of the `run` methods are invoked.
    ///
    /// - [`run_wait`](ClusterChecker::run_wait)
    /// - [`run_wait_and_fix`](ClusterChecker::run_wait_and_fix)
    /// - [`run_with_progress`](ClusterChecker::run_with_progress)
    /// - [`run_and_fix_with_progress`](ClusterChecker::run_and_fix_with_progress)
    pub fn with_k8_checks(mut self) -> Self {
        let checks: Vec<Box<(dyn ClusterCheck)>> =
            vec![Box::new(ActiveKubernetesCluster), Box::new(HelmVersion)];
        self.checks.extend(checks);
        self
    }

    /// Adds all checks required for starting a local cluster.
    ///
    /// Note that no checks are run until one of the `run` methods are invoked.
    ///
    /// - [`run_wait`](ClusterChecker::run_wait)
    /// - [`run_wait_and_fix`](ClusterChecker::run_wait_and_fix)
    /// - [`run_with_progress`](ClusterChecker::run_with_progress)
    /// - [`run_and_fix_with_progress`](ClusterChecker::run_and_fix_with_progress)
    pub fn with_local_checks(mut self) -> Self {
        let checks: Vec<Box<(dyn ClusterCheck)>> = vec![
            Box::new(HelmVersion),
            Box::new(K8Version),
            Box::new(ActiveKubernetesCluster),
            Box::new(LocalClusterCheck),
        ];
        self.checks.extend(checks);
        self
    }

    /// Performs check and fix is required
    ///
    pub async fn run(self, pb: &ProgressRenderer, fix: bool) -> Result<bool, ClusterCheckError> {
        macro_rules! pad_format {
            ( $e:expr ) => {
                format!("{:>6}", $e)
            };
        }

        // sort checks according to dependencies
        let mut components: HashSet<FluvioClusterComponent> = HashSet::new();

        let mut sorted_checks = self.checks;
        sorted_checks.sort_by(check_compare);

        let mut failed = false;
        for check in sorted_checks {
            let mut passed = false;
            let required_components = check.required_components();
            if required_components
                .iter()
                .filter(|component| components.contains(component))
                .count()
                == required_components.len()
            {
                pb.set_message(pad_format!(format!("Checking {}", check.label())));
                match check.perform_check(pb).await? {
                    CheckStatus::AutoFixableError(fixable_error) => {
                        if fix {
                            match fixable_error.attempt_fix(pb).await {
                                Ok(status) => {
                                    pb.println(format!("Fixed: {}", status));
                                    passed = true;
                                }
                                Err(err) => {
                                    // If the fix failed, wrap the original failed check in Unrecoverable
                                    pb.println(format!("Auto fix failed: {:#?}", err));
                                    failed = true;
                                }
                            }
                        } else {
                            failed = true;
                        }
                    }
                    CheckStatus::Pass(status) => {
                        passed = true;
                        pb.println(pad_format!(format!("{} {}", "✅".bold(), status)));
                    }
                    CheckStatus::Unrecoverable(_err) => {}
                }
            } else {
                pb.println("skipping check because required components are not met");
                failed = true;
            }

            if passed {
                if let Some(component) = check.component() {
                    debug!(?component, "component registered");
                    components.insert(component);
                }
            }
        }

        if !failed {
            pb.println(format!("🎉 {}", "All checks passed!".bold()));
        }

        Ok(failed)
    }
}

#[allow(clippy::borrowed_box)]
fn check_compare(first: &Box<dyn ClusterCheck>, second: &Box<dyn ClusterCheck>) -> Ordering {
    //  println!("dep1: {:#?}",dep1_set);
    //  println!("dep2: {:#?}",dep2_set);
    // check if any of dep1 is less than dep2
    if let Some(reg) = second.component() {
        //   println!("second component: {:#?}",reg);
        for dep1 in first.required_components() {
            //     println!("checking dep1: {:#?}",dep1);
            // if first is depends on second, then seconds should be listed first
            if dep1 == reg {
                return Ordering::Greater;
            }
        }
    }

    if let Some(reg) = first.component() {
        // println!("second component: {:#?}",reg);
        for dep2 in second.required_components() {
            //   println!("checking second: {:#?}",dep2);
            // if seconds is depends on first, then first should be listed first
            if dep2 == reg {
                return Ordering::Less;
            }
        }
    }

    Ordering::Equal
}

fn check_permission(resource: &str, _pb: &ProgressRenderer) -> CheckResult {
    let status = check_create_permission(resource)?;
    if !status {
        return Ok(CheckStatus::Unrecoverable(
            UnrecoverableCheckStatus::PermissionError {
                resource: resource.to_string(),
            },
        ));
    }
    Ok(CheckStatus::pass(format!("Can create {}", resource)))
}

fn check_create_permission(resource: &str) -> Result<bool, ClusterCheckError> {
    let check_command = Command::new("kubectl")
        .arg("auth")
        .arg("can-i")
        .arg("create")
        .arg(resource)
        .output()
        .map_err(ClusterCheckError::KubectlNotFoundError)?;
    let res = String::from_utf8(check_command.stdout)
        .map_err(|_| ClusterCheckError::FetchPermissionError)?;
    Ok(res.trim() == "yes")
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_check_dep() {
        let k8: Box<dyn ClusterCheck> = Box::new(super::ActiveKubernetesCluster);
        let perm: Box<dyn ClusterCheck> = Box::new(super::CreateCrdPermission);
        // since per depends on k8, k8 should be less
        assert_eq!(check_compare(&k8, &perm), Ordering::Less);
    }
}
