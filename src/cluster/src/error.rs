use std::io::Error as IoError;
use thiserror::Error;

use fluvio::FluvioError;
use k8_config::{ConfigError as K8ConfigError};
use k8_client::{ClientError as K8ClientError};
use crate::helm::HelmError;
use crate::check::CheckError;

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
    /// An error that occurred during pre-installation checks
    #[error("Fluvio pre-installation check failed")]
    PreCheckError {
        /// The pre-check error that occurred
        #[from]
        source: CheckError,
    },
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
