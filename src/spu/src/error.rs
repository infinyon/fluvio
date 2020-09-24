use std::fmt;

use async_channel::SendError;
use fluvio_types::PartitionError;
use fluvio_storage::StorageError;
use fluvio_socket::FlvSocketError;

#[derive(Debug)]
pub enum InternalServerError {
    StorageError(StorageError),
    PartitionError(PartitionError),
    SendError(String),
    SocketError(FlvSocketError),
}

impl fmt::Display for InternalServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::StorageError(err) => write!(f, "{}", err),
            Self::PartitionError(err) => write!(f, "{}", err),
            Self::SendError(err) => write!(f, "{}", err),
            Self::SocketError(err) => write!(f, "{}", err),
        }
    }
}

impl From<StorageError> for InternalServerError {
    fn from(error: StorageError) -> Self {
        InternalServerError::StorageError(error)
    }
}

impl From<PartitionError> for InternalServerError {
    fn from(error: PartitionError) -> Self {
        InternalServerError::PartitionError(error)
    }
}

impl<T> From<SendError<T>> for InternalServerError {
    fn from(error: SendError<T>) -> Self {
        InternalServerError::SendError(error.to_string())
    }
}

impl From<FlvSocketError> for InternalServerError {
    fn from(error: FlvSocketError) -> Self {
        InternalServerError::SocketError(error)
    }
}
