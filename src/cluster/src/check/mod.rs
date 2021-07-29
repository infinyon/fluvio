use std::io::Error as IoError;
use std::fmt::Debug;
use std::time::Duration;
use std::process::{Command};
use std::fs::File;

pub mod render;

use tracing::{error, warn, debug};
use async_trait::async_trait;
use async_channel::Receiver;
use url::ParseError;
use semver::Version;
use serde_json::Error as JsonError;

use fluvio_future::timer::sleep;
use fluvio_future::task::spawn;
use fluvio_helm::{HelmClient, HelmError};
use k8_config::{ConfigError as K8ConfigError, K8Config};
use k8_client::load_and_share;
use k8_types::InputObjectMeta;
use k8_types::core::service::ServiceSpec;
use k8_client::ClientError as K8ClientError;

use crate::charts::{DEFAULT_HELM_VERSION, APP_CHART_NAME};
use crate::{DEFAULT_NAMESPACE};

use crate::charts::{ChartConfig, ChartInstaller, ChartInstallError, SYS_CHART_NAME};

const DUMMY_LB_SERVICE: &str = "fluvio-dummy-service";
const DELAY: u64 = 1000;
const MINIKUBE_USERNAME: &str = "minikube";
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
pub type CheckResult = std::result::Result<CheckStatus, CheckError>;

/// A collection of the successes, failures, and errors of running checks
pub type CheckResults = Vec<CheckResult>;

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

    /// Unable to parse Error
    #[error("Could not parse Version")]
    VersionError(#[from] semver::Error),
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

impl CheckSuggestion for CheckStatus {
    fn suggestion(&self) -> Option<String> {
        match self {
            Self::Pass(_) => None,
            Self::Fail(failed) => failed.suggestion(),
        }
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

impl CheckSuggestion for CheckFailed {
    fn suggestion(&self) -> Option<String> {
        match self {
            Self::AutoRecoverable(check) => check.suggestion(),
            Self::Unrecoverable(check) => check.suggestion(),
            Self::AlreadyInstalled => None,
        }
    }
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

impl CheckSuggestion for RecoverableCheck {
    fn suggestion(&self) -> Option<String> {
        let suggestion = match self {
            Self::MissingSystemChart => "Run 'fluvio cluster start --sys'",
            Self::MinikubeTunnelNotFoundRetry => "Run 'minikube tunnel'",
        };
        Some(suggestion.to_string())
    }
}

/// A type of check failure which is not recoverable
#[derive(thiserror::Error, Debug)]
pub enum UnrecoverableCheck {
    /// We failed to recover from a potentially-recoverable check failure
    #[error("Could not repair check: {0}")]
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
    #[error("Minikube tunnel may not be running")]
    MinikubeTunnelNotFound,

    /// Default unhandled K8 client error
    #[error("Unhandled K8 client error")]
    UnhandledK8ClientError,
}

impl CheckSuggestion for UnrecoverableCheck {
    fn suggestion(&self) -> Option<String> {
        let suggestion = match self {
            Self::MinikubeTunnelNotFound => {
                "Run 'minikube tunnel >/tmp/tunnel.out 2>/tmp/tunnel.out'"
            }
            _ => return None,
        };
        Some(suggestion.to_string())
    }
}

#[async_trait]
pub trait ClusterCheck: Debug + Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> CheckResult;

    /// Attempt to fix a recoverable error.
    ///
    /// The default implementation is to fail with `FailedRecovery`. Concrete instances
    /// may override this implementation with functionality to actually attempt to fix
    /// errors.
    async fn attempt_fix(&self, check: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
        Err(UnrecoverableCheck::FailedRecovery(check))
    }
}

#[derive(Debug)]
pub(crate) struct LoadableConfig;

#[async_trait]
impl ClusterCheck for LoadableConfig {
    /// Checks that we can connect to Kubernetes via the active context
    async fn perform_check(&self) -> CheckResult {
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
            K8Config::Pod(_) => {
                return Ok(CheckStatus::pass("Pod config found, ignoring the check"))
            }
            K8Config::KubeConfig(context) => context,
        };

        let _cluster_context = match context.config.current_cluster() {
            Some(context) => context,
            None => {
                return Ok(CheckStatus::fail(
                    UnrecoverableCheck::NoActiveKubernetesContext,
                ));
            }
        };

        Ok(CheckStatus::pass("Kubernetes config is loadable"))
    }
}

#[derive(Debug)]
pub(crate) struct K8Version;

#[async_trait]
impl ClusterCheck for K8Version {
    /// Check if required kubectl version is installed
    async fn perform_check(&self) -> CheckResult {
        let kube_version = Command::new("kubectl")
            .arg("version")
            .arg("-o=json")
            .output()
            .map_err(CheckError::KubectlNotFoundError)?;

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

        let kube_versions: KubernetesVersion = serde_json::from_slice(&kube_version.stdout)
            .map_err(CheckError::KubectlVersionJsonError)?;

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
        if Version::parse(server_version)? < Version::parse(KUBE_VERSION)? {
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
}

#[derive(Debug)]
pub(crate) struct HelmVersion;

#[async_trait]
impl ClusterCheck for HelmVersion {
    /// Checks that the installed helm version is compatible with the installer requirements
    async fn perform_check(&self) -> CheckResult {
        let helm = HelmClient::new().map_err(CheckError::HelmError)?;
        let helm_version = helm.get_helm_version().map_err(CheckError::HelmError)?;
        let required = DEFAULT_HELM_VERSION;
        if Version::parse(&helm_version)? < Version::parse(required)? {
            return Ok(CheckStatus::fail(
                UnrecoverableCheck::IncompatibleHelmVersion {
                    installed: helm_version,
                    required: required.to_string(),
                },
            ));
        }
        Ok(CheckStatus::pass("Supported helm version is installed"))
    }
}

#[derive(Debug)]
pub(crate) struct SysChartCheck {
    config: ChartConfig,
}

impl SysChartCheck {
    pub(crate) fn new(config: ChartConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ClusterCheck for SysChartCheck {
    /// Check that the system chart is installed
    /// This uses whatever namespace it is being called
    async fn perform_check(&self) -> CheckResult {
        debug!("performing sys chart check");
        let helm = HelmClient::new().map_err(CheckError::HelmError)?;
        // check installed system chart version
        let sys_charts = helm
            .get_installed_chart_by_name(SYS_CHART_NAME, None)
            .map_err(CheckError::HelmError)?;
        if sys_charts.is_empty() {
            return Ok(CheckStatus::fail(RecoverableCheck::MissingSystemChart));
        } else if sys_charts.len() > 1 {
            return Ok(CheckStatus::fail(UnrecoverableCheck::MultipleSystemCharts));
        }
        Ok(CheckStatus::pass("Fluvio system charts are installed"))
    }

    async fn attempt_fix(&self, error: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
        // Use closure to catch errors
        let result = (|| -> Result<(), ChartInstallError> {
            debug!(
                "Fixing by installing Fluvio sys chart with config: {:#?}",
                &self.config
            );
            let sys_installer = ChartInstaller::from_config(self.config.clone())?;
            sys_installer.install()?;
            Ok(())
        })();
        result.map_err(|e| {
            error!("Failed to install Fluvio system chart: {:?}", e);
            UnrecoverableCheck::FailedRecovery(error)
        })?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AlreadyInstalled;

#[async_trait]
impl ClusterCheck for AlreadyInstalled {
    /// Checks that Fluvio is not already installed
    async fn perform_check(&self) -> CheckResult {
        let helm = HelmClient::new().map_err(CheckError::HelmError)?;
        let app_charts = helm
            .get_installed_chart_by_name(APP_CHART_NAME, None)
            .map_err(CheckError::HelmError)?;
        if !app_charts.is_empty() {
            return Ok(CheckStatus::fail(CheckFailed::AlreadyInstalled));
        }
        Ok(CheckStatus::pass("Previous fluvio installation not found"))
    }
}

#[derive(Debug)]
struct CreateServicePermission;

#[async_trait]
impl ClusterCheck for CreateServicePermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_SERVICE)
    }
}

#[derive(Debug)]
struct CreateCrdPermission;

#[async_trait]
impl ClusterCheck for CreateCrdPermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_CRD)
    }
}

#[derive(Debug)]
struct CreateServiceAccountPermission;

#[async_trait]
impl ClusterCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> CheckResult {
        check_permission(RESOURCE_SERVICE_ACCOUNT)
    }
}

#[derive(Debug)]
pub(crate) struct LoadBalancer;

#[async_trait]
impl ClusterCheck for LoadBalancer {
    async fn perform_check(&self) -> CheckResult {
        let config = K8Config::load().map_err(CheckError::K8ConfigError)?;
        let context = match config {
            K8Config::Pod(_) => {
                return Ok(CheckStatus::pass("Pod config found, ignoring the check"))
            }
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
        let service_exists = wait_for_service_exist(DEFAULT_NAMESPACE).await?.is_some();
        delete_service()?;

        if !service_exists {
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

    /// Attempt to fix missing load balancer by running `minikube tunnel`
    async fn attempt_fix(&self, error: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
        use std::process::Stdio;

        // Use closure to catch potential errors
        let result = (|| -> Result<(), std::io::Error> {
            let log_file = File::create("/tmp/tunnel.out")?;
            let error_file = log_file.try_clone()?;

            // run minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out
            Command::new("minikube")
                .arg("tunnel")
                .stdout(Stdio::from(log_file))
                .stderr(Stdio::from(error_file))
                .spawn()?;
            Ok(())
        })();

        if result.is_err() {
            return Err(UnrecoverableCheck::FailedRecovery(error));
        }

        sleep(Duration::from_millis(DELAY)).await;
        Ok(())
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
            Box::new(LoadableConfig),
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
            vec![Box::new(LoadableConfig), Box::new(HelmVersion)];
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
            Box::new(LoadableConfig),
        ];
        self.checks.extend(checks);
        self
    }

    /// Performs all checks sequentially and returns the results when done.
    ///
    /// This may appear to "hang" if there are many checks. In order to see
    /// fine-grained progress about ongoing checks, use [`run_with_progress`]
    /// instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::{ClusterChecker, CheckResults};
    /// # async fn do_run() {
    /// let check_results: CheckResults = ClusterChecker::empty()
    ///     .with_preflight_checks()
    ///     .run_wait()
    ///     .await;
    /// # }
    /// ```
    ///
    /// [`run_with_progress`]: ClusterChecker::run_with_progress
    pub async fn run_wait(&self) -> CheckResults {
        let mut check_results = vec![];
        for check in &self.checks {
            let result = check.perform_check().await;
            check_results.push(result);
        }
        check_results
    }

    /// Performs all checks sequentially, attempting to fix any problems along the way.
    ///
    /// This may appear to "hang" if there are many checks, or if fixes take a long time.
    pub async fn run_wait_and_fix(&self) -> CheckResults {
        // We want to collect all of the results of the checks
        let mut results: Vec<CheckResult> = vec![];

        for check in &self.checks {
            // Perform one individual check
            match check.perform_check().await {
                // If the check passed, add it to the results list
                it @ Ok(CheckStatus::Pass(_)) => results.push(it),
                // If the check failed but is potentially auto-recoverable, try to recover it
                Ok(CheckStatus::Fail(CheckFailed::AutoRecoverable(recoverable))) => {
                    let err = format!("{}", recoverable);
                    match check.attempt_fix(recoverable).await {
                        Ok(_) => {
                            results.push(Ok(CheckStatus::pass(format!("Fixed: {}", err))));
                        }
                        Err(e) => {
                            // If the fix failed, wrap the original failed check in Unrecoverable
                            results.push(Ok(CheckStatus::fail(CheckFailed::Unrecoverable(e))));
                            // We return upon the first check failure
                            return results;
                        }
                    }
                }
                it @ Ok(CheckStatus::Fail(_)) => {
                    results.push(it);
                    return results;
                }
                it @ Err(_) => {
                    results.push(it);
                    return results;
                }
            }
        }

        results
    }

    /// Performs all checks in an async task, returning the results via a channel.
    ///
    /// This function will return immediately with a channel which will yield progress
    /// updates about checks as they are run.
    ///
    /// If you want to run the checks as a single batch and receive all of the results
    /// at once, use [`run_wait`] instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::{ClusterChecker, CheckResult};
    /// # async fn do_run_with_progress() {
    /// use async_channel::Receiver;
    /// let progress: Receiver<CheckResult> = ClusterChecker::empty()
    ///     .with_preflight_checks()
    ///     .run_with_progress();
    /// while let Ok(check_result) = progress.recv().await {
    ///     println!("Got check result: {:?}", check_result);
    /// }
    /// # }
    /// ```
    ///
    /// [`run_wait`]: ClusterChecker::run_wait
    pub fn run_with_progress(self) -> Receiver<CheckResult> {
        let (sender, receiver) = async_channel::bounded(100);
        spawn(async move {
            for check in self.checks {
                let check_result = check.perform_check().await;

                // Nothing we can do if channel fails
                let progress_result = sender.send(check_result).await;

                // If channel fails, the best we can do is log it
                if let Err(error) = progress_result {
                    warn!(%error, "Failed to send check progress update to client:");
                }
            }
        });
        receiver
    }

    /// Performs all checks in an async task and attempts to fix anything it can.
    ///
    /// This function will return immediately with a channel which will yield
    /// progress updates about checks and fixes as they run.
    ///
    /// If you want to run checks and fixes as a single batch and receive all of
    /// the results at once, use [`run_wait`] instead.
    ///
    /// [`run_wait`]: ClusterChecker::run_wait
    pub fn run_and_fix_with_progress(self) -> Receiver<CheckResult> {
        let (sender, receiver) = async_channel::bounded(100);
        spawn(async move {
            for check in &self.checks {
                // Perform one individual check
                let check_result = check.perform_check().await;
                let send_result = match check_result {
                    // If the check passed, add it to the results list
                    it @ Ok(CheckStatus::Pass(_)) => sender.send(it).await,
                    // If the check failed but is potentially auto-recoverable, try to recover it
                    Ok(CheckStatus::Fail(CheckFailed::AutoRecoverable(recoverable))) => {
                        let err = format!("{}", recoverable);
                        match check.attempt_fix(recoverable).await {
                            // If the fix worked, return a passed check
                            Ok(_) => {
                                sender
                                    .send(Ok(CheckStatus::pass(format!("Fixed: {}", err))))
                                    .await
                            }
                            Err(e) => {
                                // If the fix failed, wrap the original failed check in Unrecoverable
                                sender
                                    .send(Ok(CheckStatus::fail(CheckFailed::Unrecoverable(e))))
                                    .await
                                // We return upon the first check failure
                                // return CheckResults::from(results);
                            }
                        }
                    }
                    it @ Ok(CheckStatus::Fail(_)) => {
                        let _ = sender.send(it).await;
                        return;
                    }
                    it @ Err(_) => {
                        let _ = sender.send(it).await;
                        return;
                    }
                };

                if let Err(e) = send_result {
                    warn!("Failed to send check progress update: {:?}", e);
                }
            }
        });
        receiver
    }
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
    use k8_client::meta_client::MetadataClient;
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
