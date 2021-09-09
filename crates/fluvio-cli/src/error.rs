use std::io::{Error as IoError, ErrorKind};

use semver::Version;
use fluvio::FluvioError;

#[cfg(feature = "k8s")]
use fluvio_cluster::cli::ClusterCliError;
use fluvio_sc_schema::ApiError;
use fluvio_sc_schema::errors::ErrorCode;
use fluvio_extension_common::output::OutputError;
use fluvio_socket::SocketError;
use fluvio_index::{PackageId, Target};
use crate::common::target::TargetError;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error(transparent)]
    OutputError(#[from] OutputError),

    #[cfg(feature = "k8s")]
    #[error("Fluvio cluster error")]
    ClusterCliError(#[from] ClusterCliError),

    #[error("Target Error")]
    TargetError(#[from] TargetError),
    #[error("Fluvio client error")]
    ClientError(#[from] FluvioError),

    #[cfg(feature = "k8s")]
    #[error("Kubernetes config error")]
    K8ConfigError(#[from] k8_config::ConfigError),

    #[cfg(feature = "k8s")]
    #[error("Kubernetes client error")]
    K8ClientError(#[from] k8_client::ClientError),

    #[error("Package index error")]
    IndexError(#[from] fluvio_index::Error),
    #[error("Error finding executable")]
    WhichError(#[from] which::Error),
    #[error(transparent)]
    HttpError(#[from] HttpError),
    #[error("Package {package} is not published at version {version} for target {target}")]
    PackageNotFound {
        package: PackageId,
        version: Version,
        target: Target,
    },

    #[error(transparent)]
    TlsError(#[from] fluvio_future::openssl::TlsError),

    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Unknown error: {0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
#[error("Http Error: {}", inner)]
pub struct HttpError {
    pub(crate) inner: http_types::Error,
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
            #[cfg(feature = "k8s")]
            CliError::ClusterCliError(cluster) => cluster.into_report(),
            _ => Report::from(self),
        }
    }

    /// Looks at the error value and attempts to gracefully handle reporting it
    ///
    /// Sometimes, specific errors require specific user-facing error messages.
    /// Here is where we define those messages, as well as the exit code that the
    /// program should return when exiting after those errors.
    pub fn print(self) -> Result<()> {
        match &self {
            Self::ClientError(FluvioError::AdminApi(api)) => match api {
                ApiError::Code(ErrorCode::TopicAlreadyExists, _) => {
                    println!("Topic already exists");
                    Ok(())
                }
                ApiError::Code(ErrorCode::TopicNotFound, _) => {
                    println!("Topic not found");
                    Ok(())
                }
                _ => Err(self),
            },
            #[cfg(feature = "k8s")]
            Self::ClusterCliError(ClusterCliError::TargetError(TargetError::ClientError(
                FluvioError::Socket(SocketError::Io(io)),
            ))) => match io.kind() {
                ErrorKind::ConnectionRefused => {
                    println!("Failed to connect to cluster, make sure you have started or connected to your cluster");
                    Ok(())
                }
                _ => Err(self),
            },
            _ => Err(self),
        }
    }
}
