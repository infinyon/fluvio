use std::io::Error as IoError;
use serde::export::Formatter;

use fluvio::FluvioError;
use k8_config::{ConfigError as K8ConfigError};
use k8_client::{ClientError as K8ClientError};
use crate::helm::HelmError;

/// The types of errors that can occur during cluster management
#[derive(Debug)]
pub enum ClusterError {
    /// An IO error occurred, such as opening a file or running a command.
    IoError(IoError),
    /// An error occurred with the Fluvio client.
    ClientError(FluvioError),
    /// An error occurred with the Kubernetes config.
    K8ConfigError(K8ConfigError),
    /// An error occurred with the Kubernetes client.
    K8ClientError(K8ClientError),
    /// An error occurred while running helm.
    HelmError(HelmError),
    /// The installed version of helm is incompatible
    IncompatibleHelmVersion(String, String),
    /// The fluvio-sys chart is not installed
    MissingSystemChart,
    /// Need to update minikube context
    InvalidMinikubeContext,
    /// A different kind of error occurred.
    Other(String),
}

impl std::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::ClientError(err) => write!(f, "{}", err),
            Self::K8ConfigError(err) => write!(f, "{}", err),
            Self::K8ClientError(err) => write!(f, "{}", err),
            Self::HelmError(err) => write!(f, "{}", err),
            Self::IncompatibleHelmVersion(current, min) => {
                write!(f, "Helm version {} is not compatible with fluvio platform, please install version >= {}", current, min)
            },
            Self::MissingSystemChart => write!(f, "Fluvio system chart 'fluvio-sys' is not installed"),
            Self::InvalidMinikubeContext => write!(f, "The current Kubernetes config will not work with minikube"),
            Self::Other(err) => write!(f, "{}", err),
        }
    }
}

impl From<IoError> for ClusterError {
    fn from(err: IoError) -> Self {
        Self::IoError(err)
    }
}

impl From<FluvioError> for ClusterError {
    fn from(err: FluvioError) -> Self {
        Self::ClientError(err)
    }
}

impl From<K8ConfigError> for ClusterError {
    fn from(err: K8ConfigError) -> Self {
        Self::K8ConfigError(err)
    }
}

impl From<K8ClientError> for ClusterError {
    fn from(err: K8ClientError) -> Self {
        Self::K8ClientError(err)
    }
}

impl From<HelmError> for ClusterError {
    fn from(error: HelmError) -> Self {
        Self::HelmError(error)
    }
}
