use std::io::Error as IoError;
use serde::export::Formatter;
use fluvio::ClientError;
use flv_future_aio::io::Error;
use k8_config::{ConfigError as K8ConfigError};
use k8_client::{ClientError as K8ClientError};

/// The types of errors that can occur during cluster management
#[derive(Debug)]
pub enum ClusterError {
    /// An IO error occurred, such as opening a file or running a command.
    IoError(IoError),
    /// An error occurred with the Fluvio client.
    ClientError(ClientError),
    /// An error occurred with the Kubernetes config.
    K8ConfigError(K8ConfigError),
    /// An error occurred with the Kubernetes client.
    K8ClientError(K8ClientError),
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
            Self::Other(err) => write!(f, "{}", err),
        }
    }
}

impl From<IoError> for ClusterError {
    fn from(err: Error) -> Self {
        Self::IoError(err)
    }
}

impl From<ClientError> for ClusterError {
    fn from(err: ClientError) -> Self {
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
