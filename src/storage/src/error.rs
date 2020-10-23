use std::io::Error as IoError;
use std::fmt;

use dataplane::batch::DefaultBatch;
use fluvio_future::fs::BoundedFileSinkError;
use fluvio_future::zero_copy::SendFileError;


use crate::util::OffsetError;
use crate::validator::LogValidationError;

#[derive(Debug)]
pub enum StorageError {
    IoError(IoError),
    NoRoom(DefaultBatch),
    OffsetError(OffsetError),
    LogValidationError(LogValidationError),
    SendFileError(SendFileError),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::NoRoom(batch) => write!(f, "No room {:#?}", batch),
            Self::OffsetError(err) => write!(f, "{}", err),
            Self::LogValidationError(err) => write!(f, "{}", err),
            Self::SendFileError(err) => write!(f, "{}", err),
        }
    }
}

impl From<IoError> for StorageError {
    fn from(error: IoError) -> Self {
        StorageError::IoError(error)
    }
}

impl From<BoundedFileSinkError> for StorageError {
    fn from(error: BoundedFileSinkError) -> Self {
        match error {
            BoundedFileSinkError::IoError(err) => StorageError::IoError(err),
            BoundedFileSinkError::MaxLenReached => panic!("no auto conversion for file sink error"),
        }
    }
}

impl From<OffsetError> for StorageError {
    fn from(error: OffsetError) -> Self {
        StorageError::OffsetError(error)
    }
}

impl From<LogValidationError> for StorageError {
    fn from(error: LogValidationError) -> Self {
        StorageError::LogValidationError(error)
    }
}

impl From<SendFileError> for StorageError {
    fn from(error: SendFileError) -> Self {
        StorageError::SendFileError(error)
    }
}
