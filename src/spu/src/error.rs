use async_channel::SendError;
use fluvio_types::PartitionError;
use fluvio_storage::StorageError;
use fluvio_socket::SocketError;

#[derive(Debug, thiserror::Error)]
pub enum InternalServerError {
    #[error("Storage error")]
    Storage(#[from] StorageError),
    #[error("Partition error")]
    Partition(#[from] PartitionError),
    #[error("Socket error")]
    Socket(#[from] SocketError),
    #[error("Channel send error")]
    Send(String),
}

impl<T> From<SendError<T>> for InternalServerError {
    fn from(error: SendError<T>) -> Self {
        InternalServerError::Send(error.to_string())
    }
}
