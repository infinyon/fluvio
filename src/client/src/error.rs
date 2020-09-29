use std::io::Error as IoError;
use std::path::PathBuf;
use thiserror::Error;

use fluvio_socket::FlvSocketError;
use fluvio_sc_schema::ApiError;

/// Possible errors that may arise when using Fluvio
#[derive(Error, Debug)]
pub enum FluvioError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Partition not found: {0}-{1}")]
    PartitionNotFound(String, i32),
    #[error("IO error: {source}")]
    IoError {
        #[from]
        source: IoError,
    },
    #[error("Fluvio socket error: {source}")]
    FlvSocketError {
        #[from]
        source: FlvSocketError,
    },
    #[error("Fluvio SC schema error: {source}")]
    ApiError {
        #[from]
        source: ApiError,
    },
    #[error("Unable to read profile at path {path}: {source}")]
    UnableToReadProfile {
        path: PathBuf,
        source: IoError,
    },
    #[error("Fluvio config error: {0}")]
    ConfigError(String),
    #[error("Attempted to create negative offset: {0}")]
    NegativeOffset(i64),
    #[error("Unknown error: {0}")]
    Other(String),
}
