use std::io::Error as IoError;
use std::fmt;

use dataplane_protocol::batch::DefaultBatch;
use flv_future_aio::fs::BoundedFileSinkError;
use flv_future_aio::zero_copy::SendFileError;
use kf_socket::KfSocketError;

use crate::util::OffsetError;
use crate::validator::LogValidationError;

#[derive(Debug)]
pub enum StorageError {
    IoError(IoError),
    NoRoom(DefaultBatch),
    OffsetError(OffsetError),
    LogValidationError(LogValidationError),
    SendFileError(SendFileError),
    SocketError(KfSocketError),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::NoRoom(batch) => write!(f, "No room {:#?}", batch),
            Self::OffsetError(err) => write!(f, "{}", err),
            Self::LogValidationError(err) => write!(f, "{}", err),
            Self::SocketError(err) => write!(f, "{}", err),
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

impl From<KfSocketError> for StorageError {
    fn from(error: KfSocketError) -> Self {
        StorageError::SocketError(error)
    }
}
