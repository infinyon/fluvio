use std::{convert::Infallible};

use handlebars::TemplateError;
use indicatif::style::TemplateError as ProgressTemplateError;

use fluvio::FluvioError;
#[cfg(feature = "k8s")]
use fluvio_cluster::cli::ClusterCliError;
use fluvio_sc_schema::errors::ErrorCode;
use fluvio_extension_common::output::OutputError;

use crate::common::target::TargetError;

#[derive(thiserror::Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum CliError {
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

    #[error("Package error: {0}")]
    PackageError(String),

    #[error(transparent)]
    TlsError(#[from] fluvio_future::openssl::TlsError),

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
    #[error("Not active profile set in config")]
    NoActiveProfileInConfig,
    #[error("Profile not found in config: {0}")]
    ProfileNotFoundInConfig(String),
    #[error("Cluster not found in config: {0}")]
    ClusterNotFoundInConfig(String),
    #[error("Progress Error")]
    ProgressError(#[from] ProgressTemplateError),
    #[cfg(feature = "smartengine")]
    #[error("SmartModuleEngine config: {0}")]
    SmartModuleConfigBuilder(#[from] fluvio_smartengine::SmartModuleConfigBuilderError),
    #[error("Hub error: {0}")]
    HubError(String),
}
