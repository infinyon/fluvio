use std::io::Error as IoError;

use fluvio_future::fs::BoundedFileSinkError;
use fluvio_future::zero_copy::SendFileError;

use crate::util::OffsetError;
use crate::validator::LogValidationError;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Other error: {0}")]
    Other(String),

    #[error(transparent)]
    Io(#[from] IoError),
    #[error("Offset error: {0}")]
    Offset(#[from] OffsetError),
    #[error("Log validation error: {0}")]
    LogValidation(#[from] LogValidationError),
    #[error("Zero-copy send file error: {0}")]
    SendFile(#[from] SendFileError),
    #[error("Batch exceeded maximum bytes: {0}")]
    BatchTooBig(usize),
    #[error("Batch size {batch_size} exceeded max segment size {max_segment_size}")]
    BatchExceededSegment {
        batch_size: u64,
        max_segment_size: u64,
    },
    #[error("Batch is empty")]
    EmptyBatch,
}

impl From<BoundedFileSinkError> for StorageError {
    fn from(error: BoundedFileSinkError) -> Self {
        match error {
            BoundedFileSinkError::IoError(err) => StorageError::Io(err),
            BoundedFileSinkError::MaxLenReached => panic!("no auto conversion for file sink error"),
        }
    }
}
