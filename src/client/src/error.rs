use std::io::Error as IoError;

use fluvio_socket::SocketError;
use fluvio_sc_schema::ApiError;
use crate::config::ConfigError;
use semver::Version;

/// Possible errors that may arise when using Fluvio
#[derive(thiserror::Error, Debug)]
pub enum FluvioError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Partition not found: {0}-{1}")]
    PartitionNotFound(String, i32),
    #[error("Spu not found: {0}")]
    SPUNotFound(i32),
    #[error("Fluvio socket error")]
    Socket(#[from] SocketError),
    #[error("Fluvio SC schema error")]
    ScSchema(#[from] ApiError),
    #[error("Fluvio config error")]
    ClientConfig(#[from] ConfigError),
    #[error("Attempted to create negative offset: {0}")]
    NegativeOffset(i64),
    #[error("Cluster (with platform version {cluster_version}) is older than the minimum required version {client_minimum_version}")]
    MinimumPlatformVersion {
        cluster_version: Version,
        client_minimum_version: Version,
    },
    #[error("Consumer config error: {0}")]
    ConsumerConfig(String),
    #[error("Unknown error: {0}")]
    Other(String),
}
