use std::fmt;

use futures::channel::mpsc::SendError;
use fluvio_types::PartitionError;
use fluvio_storage::StorageError;
use kf_socket::KfSocketError;

#[derive(Debug)]
pub enum InternalServerError {
    StorageError(StorageError),
    PartitionError(PartitionError),
    SendError(SendError),
    SocketError(KfSocketError),
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

impl From<SendError> for InternalServerError {
    fn from(error: SendError) -> Self {
        InternalServerError::SendError(error)
    }
}

impl From<KfSocketError> for InternalServerError {
    fn from(error: KfSocketError) -> Self {
        InternalServerError::SocketError(error)
    }
}
