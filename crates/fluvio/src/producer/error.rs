use async_channel::RecvError;
use fluvio_future::retry::TimeoutError;
use fluvio_protocol::link::ErrorCode;

use super::record::RecordMetadata;

use crate::producer::PartitionId;

#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum ProducerError {
    #[error("record size ({0} bytes), exceeded maximum request size ({0} bytes)")]
    RecordTooLarge(usize, usize),
    #[error("failed to send record metadata: {0}")]
    SendRecordMetadata(#[from] async_channel::SendError<RecordMetadata>),
    #[error("failed to get record metadata")]
    GetRecordMetadata(#[from] Option<RecvError>),
    #[error("partition: {0} not found")]
    PartitionNotFound(PartitionId),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Producer received an error code: {0}")]
    SpuErrorCode(#[from] ErrorCode),
    #[error("Invalid configuration in producer: {0}")]
    InvalidConfiguration(String),
    #[error("the produce request retry timeout limit reached")]
    ProduceRequestRetryTimeout(#[from] TimeoutError),
    #[error("the batch enqueue timeout limit reached")]
    BatchQueueWaitTimeout,
}
