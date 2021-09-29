use crate::producer::assoc::BatchFailure;

#[derive(thiserror::Error, Debug)]
pub enum ProducerError {
    #[error("failed to flush a batch of records")]
    Flush,
    #[error("failed to send a batch of records")]
    BatchFailed(BatchFailure),
    #[error("failed to associate record IDs with a batch")]
    BatchNotFound,
    #[error("the given record is larger than the buffer max_size ({0} bytes)")]
    RecordTooLarge(usize),
    #[error("internal error: {0}")]
    Internal(String),
}

impl<T> From<flume::SendError<T>> for ProducerError {
    fn from(_: flume::SendError<T>) -> Self {
        Self::Internal("failed to send message through channel (flume)".to_string())
    }
}

impl<T> From<tokio::sync::broadcast::error::SendError<T>> for ProducerError {
    fn from(_: tokio::sync::broadcast::error::SendError<T>) -> Self {
        Self::Internal("failed to send message through channel (tokio broadcast)".to_string())
    }
}
