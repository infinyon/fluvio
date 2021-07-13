use std::io::Error as IoError;
use thiserror::Error;

use fluvio_socket::FlvSocketError;
use fluvio_sc_schema::ApiError;
use crate::config::ConfigError;
use semver::Version;

/// Possible errors that may arise when using Fluvio
#[derive(Error, Debug)]
pub enum FluvioError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Partition not found: {0}-{1}")]
    PartitionNotFound(String, i32),
    #[error("Spu not found: {0}")]
    SPUNotFound(i32),
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Fluvio socket error")]
    FlvSocketError(#[from] FlvSocketError),
    #[error("Fluvio SC schema error")]
    ApiError(#[from] ApiError),
    #[error("Fluvio config error")]
    ConfigError(#[from] ConfigError),
    #[error("Attempted to create negative offset: {0}")]
    NegativeOffset(i64),
    #[error("Cluster (with platform version {cluster_version}) is older than the minimum required version {client_minimum_version}")]
    MinimumPlatformVersion {
        cluster_version: Version,
        client_minimum_version: Version,
    },
    #[error("User SmartStream failed with the following error: {0}")]
    SmartStreamUserError(String),
    #[error("Unknown error: {0}")]
    Other(String),
}
