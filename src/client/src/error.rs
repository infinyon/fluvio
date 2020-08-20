use std::fmt;
use std::io::Error as IoError;

use kf_protocol::api::ReplicaKey;
use kf_socket::KfSocketError;
use fluvio_controlplane_api::ApiError;

#[derive(Debug)]
pub enum ClientError {
    TopicNotFound(String),
    PartitionNotFound(ReplicaKey),
    Other(String),
    IoError(IoError),
    KfSocketError(KfSocketError),
    ApiError(ApiError),
    UnableToReadProfile,
}

impl From<IoError> for ClientError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<KfSocketError> for ClientError {
    fn from(error: KfSocketError) -> Self {
        Self::KfSocketError(error)
    }
}

impl From<ApiError> for ClientError {
    fn from(error: ApiError) -> Self {
        Self::ApiError(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TopicNotFound(topic) => write!(f, "topic: {} not found", topic),
            Self::PartitionNotFound(replica) => write!(f, "partition <{}> not found", replica),
            Self::Other(msg) => write!(f, "{}", msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::KfSocketError(err) => write!(f, "{:#?}", err),
            Self::ApiError(err) => write!(f, "{}", err),
            Self::UnableToReadProfile => write!(f, "No configuration has been provided"),
        }
    }
}
