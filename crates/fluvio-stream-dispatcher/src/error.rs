// error.rs
//  Server Error handling (union of errors used by server)
//

use std::fmt;
use std::io::Error as StdIoError;

use async_channel::SendError;
use fluvio_types::PartitionError;

#[derive(Debug)]
pub enum StoreError {
    IoError(StdIoError),
    SendError(String),
    PartitionError(PartitionError),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{err}"),
            Self::SendError(err) => write!(f, "{err}"),
            Self::PartitionError(err) => write!(f, "{err}"),
        }
    }
}

impl From<StdIoError> for StoreError {
    fn from(error: StdIoError) -> Self {
        Self::IoError(error)
    }
}

impl<T> From<SendError<T>> for StoreError {
    fn from(error: SendError<T>) -> Self {
        Self::SendError(error.to_string())
    }
}

impl From<PartitionError> for StoreError {
    fn from(error: PartitionError) -> Self {
        Self::PartitionError(error)
    }
}
