use std::{
    convert::Infallible,
    io::{Error as IoError, ErrorKind},
};

use semver::Version;
use handlebars::TemplateError;
use indicatif::style::TemplateError as ProgressTemplateError;

use fluvio::FluvioError;
#[cfg(feature = "k8s")]
use fluvio_cluster::cli::ClusterCliError;
use fluvio_sc_schema::ApiError;
use fluvio_sc_schema::errors::ErrorCode;
use fluvio_extension_common::output::OutputError;
use fluvio_socket::SocketError;
use fluvio_index::{PackageId, Target};
use crate::common::target::TargetError;

pub type Result<T, E = CliError> = core::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error(transparent)]
    OutputError(#[from] OutputError),
    #[error("Failed to parse format string")]
    TemplateError(#[from] TemplateError),

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

    /// An error occurred while processing the connector yaml
    #[error("Fluvio connector config")]
    ConnectorConfig(#[from] serde_yaml::Error),

    #[error("Package index error")]
    IndexError(#[from] fluvio_index::Error),
    #[error("Error finding executable")]
    WhichError(#[from] which::Error),
    #[error("Http Error: {0}")]
    HttpError(#[from] fluvio_cli_common::error::HttpError),
    #[error("Package {package} is not published at version {version} for target {target}")]
    PackageNotFound {
        package: PackageId,
        version: Version,
        target: Target,
    },

    #[error(transparent)]
    TlsError(#[from] fluvio_future::openssl::TlsError),

    #[error("Invalid connector: {0}")]
    InvalidConnector(String),
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Unknown error: {0}")]
    Other(String),
    #[error("{0}")]
    CollectedError(String),
    #[error("Unexpected Infallible error")]
    Infallible(#[from] Infallible),
    #[error("Dataplane error: {0}")]
    DataPlaneError(#[from] ErrorCode),
    #[error("TableFormat not found: {0}")]
    TableFormatNotFound(String),
    #[error(transparent)]
    FluvioInstall(#[from] fluvio_cli_common::error::CliError),
    #[error("Not active profile set in config")]
    NoActiveProfileInConfig,
    #[error("Profile not found in config: {0}")]
    ProfileNotFoundInConfig(String),
    #[error("Cluster not found in config: {0}")]
    ClusterNotFoundInConfig(String),
    #[error("Connector not found: {0}")]
    ConnectorNotFound(String),
    #[error("Progress Error")]
    ProgressError(#[from] ProgressTemplateError),
    #[cfg(feature = "smartengine")]
    #[error("SmartModuleEngine config: {0}")]
    SmartModuleConfigBuilder(#[from] fluvio_smartengine::SmartModuleConfigBuilderError),
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

    /// Looks at the error value and attempts to create a user facing error message
    ///
    /// Sometimes, specific errors require specific user-facing error messages.
    /// Here is where we define those messages, as well as the exit code that the
    /// program should return when exiting after those errors.
    pub fn get_user_error(self) -> Result<&'static str> {
        match &self {
            Self::ClientError(FluvioError::AdminApi(api)) => match api {
                ApiError::Code(ErrorCode::TopicAlreadyExists, _) => {
                    Ok("Topic already exists")
                }
                ApiError::Code(ErrorCode::ManagedConnectorAlreadyExists, _) => {
                    Ok("Connector already exists")
                }
                ApiError::Code(ErrorCode::TopicNotFound, _) => Ok("Topic not found"),
                ApiError::Code(ErrorCode::SmartModuleNotFound{ name: _ }, _) => Ok("SmartModule not found"),
                ApiError::Code(ErrorCode::ManagedConnectorNotFound, _) => {
                    Ok("Connector not found")
                }
                ApiError::Code(ErrorCode::TopicInvalidName, _) => {
                    Ok("Invalid topic name: topic name may only include lowercase letters (a-z), numbers (0-9), and hyphens (-).")
                }
                ApiError::Code(ErrorCode::TableFormatAlreadyExists, _) => {
                    Ok("TableFormat already exists")
                }
                ApiError::Code(ErrorCode::TableFormatNotFound, _) => {
                    Ok("TableFormat not found")
                }
                _ => Err(self),
            },
            Self::ClientError(FluvioError::Socket(SocketError::Io{ source, ..}))
                if source.kind() == ErrorKind::TimedOut =>
            {
                Ok("Network connection timed out while waiting for response")
            }
            #[cfg(feature = "k8s")]
            Self::ClusterCliError(ClusterCliError::TargetError(TargetError::ClientError(
                FluvioError::Socket(SocketError::Io{ source, .. }),
            ))) => match source.kind() {
                ErrorKind::ConnectionRefused => {
                    Ok("Failed to connect to cluster, make sure you have started or connected to your cluster")
                }
                _ => Err(self),
            },
            _ => Err(self),
        }
    }
}
