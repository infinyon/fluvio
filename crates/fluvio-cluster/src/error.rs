use std::io::Error as IoError;
use std::process::ExitStatus;

use anyhow::Error as AnyError;
use indicatif::style::TemplateError;

use fluvio::FluvioError;
use k8_config::{ConfigError as K8ConfigError};
use k8_client::{ClientError as K8ClientError};
use fluvio_helm::HelmError;
use fluvio_command::CommandError;

use crate::check::{CheckResults, CheckStatuses, ClusterCheckError};
use crate::charts::ChartInstallError;
use crate::runtime::local::LocalRuntimeError;

/// The types of errors that can occur during cluster management
#[derive(thiserror::Error, Debug)]
pub enum ClusterError {
    /// An error occurred while trying to install Fluvio on Kubernetes
    #[error("Failed to install Fluvio on Kubernetes")]
    InstallK8(#[from] K8InstallError),
    /// An error occurred while trying to install Fluvio locally
    #[error("Failed to install Fluvio locally")]
    InstallLocal(#[from] LocalInstallError),
    /// An error occurred while trying to install Fluvio system charts
    #[error("Failed to install Fluvio system charts")]
    InstallSys(#[from] ChartInstallError),
    /// An error occurred while trying to uninstall Fluvio
    #[error("Failed to uninstall Fluvio")]
    Uninstall(#[from] UninstallError),
    #[error("Progress Error")]
    ProgressError(#[from] TemplateError),
}

/// Errors that may occur while trying to install Fluvio on Kubernetes
#[derive(thiserror::Error, Debug)]
pub enum K8InstallError {
    /// An error occurred with the Kubernetes config.
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] K8ConfigError),
    /// An error occurred with the Kubernetes client.
    #[error("Kubernetes client error")]
    K8ClientError(#[from] K8ClientError),
    /// An error occurred while running helm.
    #[error("Helm client error")]
    HelmError(#[from] HelmError),
    /// An error occurred while running helm.
    #[error("Helm Chart error")]
    ChartError(#[from] ChartInstallError),
    /// Failed to execute a command
    #[error(transparent)]
    CommandError(#[from] CommandError),
    /// One or more pre-checks (successfully) failed when trying to start the cluster
    #[error("Pre-checks failed during cluster startup")]
    FailedPrecheck(CheckStatuses),
    /// Encountered an error while performing one or more pre-checks
    #[error("Failed to perform one or more pre-checks")]
    PrecheckErrored(CheckResults),
    /// Failed to update Fluvio cluster
    #[error("Expected to find cluster with platform version {0}")]
    FailedPlatformVersion(String),
    /// Timed out when waiting for SC service.
    #[error("Timed out when waiting for SC service")]
    SCServiceTimeout,
    /// Timed out when waiting for SC deployment availability.
    #[error("Timed out when waiting for SC deployment")]
    SCDeploymentTimeout,
    /// Timed out when waiting for SC port check.
    #[error("Timed out when waiting for SC port check")]
    SCPortCheckTimeout,
    /// Unable to find a valid ingress address for SC service
    #[error("Unable to find a valid ingress address for SC service")]
    SCIngressNotValid,
    /// Timed out when waiting for DNS resolution.
    #[error("Timed out when waiting for DNS resolution")]
    SCDNSTimeout,
    /// Timed out when waiting for SPU.
    #[error("Timed out when waiting for SPU")]
    SPUTimeout,
    /// Unable to find Fluvio SC service in Kubernetes
    #[error("Unable to detect Fluvio SC K8 service")]
    UnableToDetectService,
    /// Unable to find a needed Helm chart
    #[error("Unable to find chart in Helm: {0}")]
    HelmChartNotFound(String),
    /// Attempted to construct a Config object without all required fields
    #[error("Missing required config option {0}")]
    MissingRequiredConfig(String),
    /// Kubectl not found
    #[error("kubectl not found")]
    KubectlNotFoundError(IoError),
    /// Kubectl not found
    #[error("Port forwarding process exited with code: {0}")]
    PortForwardingFailed(ExitStatus),
    #[error("Progress Error")]
    ProgressError(#[from] TemplateError),
}

/// Errors that may occur while trying to install Fluvio locally
#[derive(thiserror::Error, Debug)]
pub enum LocalInstallError {
    /// An IO error occured during log dir creation
    #[error("An error occurred creating the cluster log directory {path:?}")]
    LogDirectoryError {
        path: std::path::PathBuf,
        source: IoError,
    },
    /// An IO error occurred, such as opening a file or running a command.
    #[error(transparent)]
    IoError(#[from] IoError),
    /// An error occurred with the Fluvio client.
    #[error("Fluvio client error")]
    FluvioError(#[from] FluvioError),
    /// An error occurred with the Kubernetes config.
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] K8ConfigError),
    /// An error occurred with the Kubernetes client.
    #[error("Kubernetes client error")]
    K8ClientError(#[from] K8ClientError),
    /// An error occurred while running helm.
    #[error("Helm client error")]
    HelmError(#[from] HelmError),
    /// Failed to execute a command
    #[error(transparent)]
    CommandError(#[from] CommandError),
    /// One or more pre-checks (successfully) failed when trying to start the cluster
    #[error("Pre-checks failed during cluster startup")]
    FailedPrecheck(CheckStatuses),
    /// Encountered an error while performing one or more pre-checks
    #[error("Failed to perform one or more pre-checks")]
    PrecheckErrored(CheckResults),
    /// Timed out when waiting for SC service.
    #[error("Timed out when waiting for SC service")]
    SCServiceTimeout,
    /// Timed out when waiting for SC port check.
    #[error("Timed out when waiting for SC port check")]
    SCPortCheckTimeout,
    /// Timed out when waiting for DNS resolution.
    #[error("Timed out when waiting for DNS resolution")]
    SCDNSTimeout,
    /// Timed out when waiting for SPU.
    #[error("Timed out when waiting for SPU")]
    SPUTimeout,
    /// Unable to find Fluvio SC service in Kubernetes
    #[error("Unable to detect Fluvio SC K8 service")]
    UnableToDetectService,
    /// Unable to find a needed Helm chart
    #[error("Unable to find chart in Helm: {0}")]
    HelmChartNotFound(String),
    /// Attempted to construct a Config object without all required fields
    #[error("Missing required config option {0}")]
    MissingRequiredConfig(String),
    #[error(transparent)]
    RuntimeError(#[from] LocalRuntimeError),
    #[error(transparent)]
    SpuClusterError(#[from] AnyError),
    /// A different kind of error occurred.
    #[error("An unknown error occurred: {0}")]
    Other(String),
    #[error(transparent)]
    ClusterCheckError(#[from] ClusterCheckError),
    #[error("Progress Error")]
    ProgressError(#[from] TemplateError),
}

/// Errors that may occur while trying to unintsall Fluvio
#[derive(thiserror::Error, Debug)]
pub enum UninstallError {
    /// An IO error occurred, such as opening a file or running a command.
    #[error(transparent)]
    IoError(#[from] IoError),
    /// An error occurred with the Fluvio client.
    #[error("Fluvio client error")]
    FluvioError(#[from] FluvioError),
    /// Failed to execute a command
    #[error(transparent)]
    CommandError(#[from] CommandError),
    /// An error occurred with the Kubernetes config.
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] K8ConfigError),
    /// An error occurred with the Kubernetes client.
    #[error("Kubernetes client error")]
    K8ClientError(#[from] K8ClientError),
    /// An error occurred while running helm.
    #[error("Helm client error")]
    HelmError(#[from] HelmError),
    /// Timed out when waiting for SC service.
    #[error("Timed out when waiting for SC service")]
    SCServiceTimeout,
    /// Timed out when waiting for SC port check.
    #[error("Timed out when waiting for SC port check")]
    SCPortCheckTimeout,
    /// Timed out when waiting for DNS resolution.
    #[error("Timed out when waiting for DNS resolution")]
    SCDNSTimeout,
    /// Timed out when waiting for SPU.
    #[error("Timed out when waiting for SPU")]
    SPUTimeout,
    /// Unable to find Fluvio SC service in Kubernetes
    #[error("Unable to detect Fluvio SC K8 service")]
    UnableToDetectService,
    /// Unable to find a needed Helm chart
    #[error("Unable to find chart in Helm: {0}")]
    HelmChartNotFound(String),
    /// A different kind of error occurred.
    #[error("An unknown error occurred: {0}")]
    Other(String),
}
