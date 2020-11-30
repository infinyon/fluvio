use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;
use fluvio_extension_consumer::ConsumerError;
use fluvio_cluster::cli::ClusterCliError;
use crate::common::target::TargetError;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error(transparent)]
    OutputError(#[from] OutputError),
    #[error("Fluvio cluster error")]
    ClusterCliError(#[from] ClusterCliError),
    #[error("Target Error")]
    TargetError(#[from] TargetError),
    #[error("Consumer Error")]
    ConsumerError(#[from] ConsumerError),
    #[error("Fluvio client error")]
    ClientError(#[from] FluvioError),
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] k8_config::ConfigError),
    #[error("Kubernetes client error")]
    K8ClientError(#[from] k8_client::ClientError),
    #[error("Package index error")]
    IndexError(#[from] fluvio_index::Error),
    #[error("Error finding executable")]
    WhichError(#[from] which::Error),
    #[error(transparent)]
    HttpError(#[from] HttpError),
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Unknown error: {0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
#[error("Http Error: {}", inner)]
pub struct HttpError {
    inner: http_types::Error,
}

impl From<http_types::Error> for CliError {
    fn from(inner: http_types::Error) -> Self {
        Self::HttpError(HttpError { inner })
    }
}

impl CliError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }

    pub fn into_report(self) -> color_eyre::Report {
        use color_eyre::Report;

        match self {
            CliError::ClusterCliError(cluster) => cluster.into_report(),
            _ => Report::from(self),
        }
    }
}
