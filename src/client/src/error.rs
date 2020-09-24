use std::fmt;
use std::io::Error as IoError;

use fluvio_socket::FlvSocketError;
use fluvio_sc_schema::ApiError;

/// Possible errors that may arise when using Fluvio
#[derive(Debug)]
pub enum FluvioError {
    TopicNotFound(String),
    PartitionNotFound(String, i32),
    Other(String),
    IoError(IoError),
    FlvSocketError(FlvSocketError),
    ApiError(ApiError),
    UnableToReadProfile,
    ConfigError(String),
    NegativeOffset(i64),
}

impl From<IoError> for FluvioError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<FlvSocketError> for FluvioError {
    fn from(error: FlvSocketError) -> Self {
        Self::FlvSocketError(error)
    }
}

impl From<ApiError> for FluvioError {
    fn from(error: ApiError) -> Self {
        Self::ApiError(error)
    }
}

impl fmt::Display for FluvioError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TopicNotFound(topic) => write!(f, "topic: {} not found", topic),
            Self::PartitionNotFound(topic, partition) => {
                write!(f, "partition <{}-{}> not found", topic, partition)
            }
            Self::Other(msg) => write!(f, "{}", msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::FlvSocketError(err) => write!(f, "{:#?}", err),
            Self::ApiError(err) => write!(f, "{}", err),
            Self::UnableToReadProfile => write!(f, "No configuration has been provided"),
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::NegativeOffset(err) => write!(f, "Negative offsets are illegal. Got: {}", err),
        }
    }
}
