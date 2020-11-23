use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_cluster::{ClusterError, CheckError};
use fluvio_cluster::extension::ClusterCmdError;
use fluvio_extension_consumer::ConsumerError;

use crate::common::output::OutputError;
use crate::common::target::TargetError;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError {
        #[from]
        source: IoError,
    },
    #[error(transparent)]
    OutputError {
        #[from]
        source: OutputError,
    },
    #[error("Consumer Error")]
    ConsumerError {
        #[from]
        source: ConsumerError,
    },
    #[error("ClusterCommand Error")]
    ClusterCmdError {
        #[from]
        source: ClusterCmdError,
    },
    #[error("ClusterCommand Error")]
    TargetError {
        #[from]
        source: TargetError,
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
    #[error("Package index error")]
    IndexError {
        #[from]
        source: fluvio_index::Error,
    },
    #[error("Error finding executable")]
    WhichError {
        #[from]
        source: which::Error,
    },
    #[error(transparent)]
    HttpError {
        #[from]
        source: HttpError,
    },
    #[error("Unknown error: {0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
#[error("Http Error: {}", inner)]
pub struct HttpError {
    inner: http_types::Error,
}

impl From<http_types::Error> for CliError {
    fn from(e: http_types::Error) -> Self {
        CliError::HttpError {
            source: HttpError { inner: e },
        }
    }
}

impl CliError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }
}
