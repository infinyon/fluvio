use async_channel::SendError;

pub type Result<T, E = ProducerError> = core::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum ProducerError {
    #[error("failed to flush a batch of records")]
    Flush,
    #[error("failed to send a message in an internal channel")]
    ChannelSend,
}

impl<T> From<SendError<T>> for ProducerError {
    fn from(_: SendError<T>) -> Self {
        Self::ChannelSend
    }
}
