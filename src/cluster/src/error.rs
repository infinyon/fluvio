use std::io::Error as IoError;
use thiserror::Error;

use fluvio::FluvioError;
use k8_config::{ConfigError as K8ConfigError};
use k8_client::{ClientError as K8ClientError};
use crate::helm::HelmError;

/// The types of errors that can occur during cluster management
#[derive(Error, Debug)]
pub enum ClusterError {
    /// An IO error occurred, such as opening a file or running a command.
    #[error(transparent)]
    IoError {
        #[from]
        /// The underlying IO error
        source: IoError,
    },
    /// An error occurred with the Fluvio client.
    #[error("Fluvio client error")]
    FluvioError {
        #[from]
        /// The underlying Fluvio error
        source: FluvioError,
    },
    /// An error occurred with the Kubernetes config.
    #[error("Kubernetes config error")]
    K8ConfigError {
        #[from]
        /// The underlying Kubernetes config error
        source: K8ConfigError,
    },
    /// An error occurred with the Kubernetes client.
    #[error("Kubernetes client error")]
    K8ClientError {
        #[from]
        /// The underlying Kubernetes client error
        source: K8ClientError,
    },
    /// An error occurred while running helm.
    #[error("Helm client error")]
    HelmError {
        #[from]
        /// The underlying Helm client error
        source: HelmError,
    },
    /// The installed version of helm is incompatible
    #[error("Must have helm version {required} or later. You have {installed}")]
    IncompatibleHelmVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },
    /// The fluvio-sys chart is not installed
    #[error("The fluvio-sys chart is not installed")]
    MissingSystemChart,
    /// Fluvio is already correctly installed
    #[error("The fluvio-app chart is already installed")]
    AlreadyInstalled,
    /// Need to update minikube context
    #[error("The minikube context is not active or does not match your minikube ip")]
    InvalidMinikubeContext,
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
    /// A different kind of error occurred.
    #[error("An unknown error occurred: {0}")]
    Other(String),
}
