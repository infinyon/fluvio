use crate::producer::assoc::BatchFailure;

#[derive(thiserror::Error, Debug)]
pub enum ProducerError {
    #[error("failed to flush a batch of records")]
    Flush,
    #[error("failed to send a batch of records")]
    BatchFailed(BatchFailure),
    #[error("failed to associate record IDs with a batch")]
    BatchNotFound,
    #[error("the given record is larger than the buffer max_size")]
    RecordTooLarge,
    #[error("failed to send a message in an internal channel (async_channel)")]
    AsyncChannelSend,
    #[error("failed to send a message in an internal channel (broadcast)")]
    BroadcastChannelSend,
}

impl<T> From<async_channel::SendError<T>> for ProducerError {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self::AsyncChannelSend
    }
}

impl<T> From<tokio::sync::broadcast::error::SendError<T>> for ProducerError {
    fn from(_: tokio::sync::broadcast::error::SendError<T>) -> Self {
        Self::BroadcastChannelSend
    }
}
