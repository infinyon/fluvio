use std::fmt;
use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_cluster::ClusterError;
use crate::profile::CloudError;

#[derive(Debug)]
pub enum CliError {
    InvalidArg(String),
    IoError(IoError),
    ClientError(FluvioError),
    CloudError(CloudError),
    ClusterError(ClusterError),
    K8ConfigError(k8_config::ConfigError),
    K8ClientError(k8_client::ClientError),
    Other(String),
}

impl CliError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }
}

impl From<IoError> for CliError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<FluvioError> for CliError {
    fn from(error: FluvioError) -> Self {
        Self::ClientError(error)
    }
}

impl From<CloudError> for CliError {
    fn from(error: CloudError) -> Self {
        Self::CloudError(error)
    }
}

impl From<ClusterError> for CliError {
    fn from(error: ClusterError) -> Self {
        Self::ClusterError(error)
    }
}

impl From<k8_config::ConfigError> for CliError {
    fn from(error: k8_config::ConfigError) -> Self {
        Self::K8ConfigError(error)
    }
}

impl From<k8_client::ClientError> for CliError {
    fn from(error: k8_client::ClientError) -> Self {
        Self::K8ClientError(error)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidArg(msg) => write!(f, "{}", msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::ClientError(err) => write!(f, "{}", err),
            Self::CloudError(err) => write!(f, "{}", err),
            Self::ClusterError(err) => write!(f, "{}", err),
            Self::K8ConfigError(err) => write!(f, "{}", err),
            Self::K8ClientError(err) => write!(f, "{}", err),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}
