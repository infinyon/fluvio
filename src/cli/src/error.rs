use std::io::Error as IoError;
use thiserror::Error;

use fluvio::FluvioError;
use fluvio_cluster::{ClusterError, CheckError};

#[derive(Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError {
        #[from]
        source: IoError,
    },
    #[error("Fluvio client error")]
    ClientError {
        #[from]
        source: FluvioError,
    },
    #[error("Fluvio cluster error")]
    ClusterError {
        #[from]
        source: ClusterError,
    },
    #[error("Fluvio cluster pre install check error")]
    CheckError {
        #[from]
        source: CheckError,
    },
    #[error("Kubernetes config error")]
    K8ConfigError {
        #[from]
        source: k8_config::ConfigError,
    },
    #[error("Kubernetes client error")]
    K8ClientError {
        #[from]
        source: k8_client::ClientError,
    },
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Error finding executable")]
    WhichError {
        #[from]
        source: which::Error,
    },
    #[error("Unknown error: {0}")]
    Other(String),
}

impl CliError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }
}
