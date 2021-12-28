use std::io::Error as IoError;

use fluvio_socket::SocketError;
use fluvio_sc_schema::ApiError;
use semver::Version;
use dataplane::smartmodule::SmartModuleRuntimeError;

use crate::config::ConfigError;
use crate::producer::ProducerError;
use crate::producer::TopicProducerConfigBuilderError;

pub type Result<T, Err = FluvioError> = std::result::Result<T, Err>;

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
    #[error("Fluvio socket error: {0}")]
    Socket(#[from] SocketError),
    #[error("Fluvio controlplane error: {0}")]
    AdminApi(#[from] ApiError),
    #[error("Fluvio config error: {0}")]
    ClientConfig(#[from] ConfigError),
    #[error("Attempted to create negative offset: {0}")]
    NegativeOffset(i64),
    #[error("Cluster (with platform version {cluster_version}) is older than the minimum required version {client_minimum_version}
To interact with this cluster, please install the matching CLI version using the following command:
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION={cluster_version} bash
    ")]
    MinimumPlatformVersion {
        cluster_version: Version,
        client_minimum_version: Version,
    },
    #[error("Cluster (with platform version {cluster_version}) is newer than this CLI major version {client_maximum_version}
To interact with this cluster, please install the matching CLI version using the following command:
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION={cluster_version} bash
    ")]
    MaximumPlatformVersion {
        cluster_version: Version,
        client_maximum_version: Version,
    },
    #[error("Consumer config error: {0}")]
    ConsumerConfig(String),
    #[error("SmartModule runtime error {0}")]
    SmartModuleRuntime(#[from] SmartModuleRuntimeError),
    #[error("Producer error: {0}")]
    Producer(#[from] ProducerError),
    #[error("Error building producer config: {0}")]
    TopicProducerConfigBuilder(#[from] TopicProducerConfigBuilderError),
    #[error("Unknown error: {0}")]
    Other(String),
}
