// error.rs
//  Server Error handling (union of errors used by server)
//

use std::fmt;
use std::io::Error as StdIoError;
use futures::channel::mpsc::SendError;

use kf_socket::KfSocketError;
use flv_types::PartitionError;

#[derive(Debug)]
pub enum ScServerError {
    IoError(StdIoError),
    SendError(SendError),
    SocketError(KfSocketError),
    PartitionError(PartitionError),

}

impl fmt::Display for ScServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::SendError(err) => write!(f, "{}", err),
            Self::SocketError(err) => write!(f, "{}", err),
            Self::PartitionError(err) => write!(f, "{}", err)
        }
    }
}

impl From<StdIoError> for ScServerError {
    fn from(error: StdIoError) -> Self {
        Self::IoError(error)
    }
}

impl From<KfSocketError> for ScServerError {
    fn from(error: KfSocketError) -> Self {
        Self::SocketError(error)
    }
}

impl From<SendError> for ScServerError {
    fn from(error: SendError) -> Self {
        Self::SendError(error)
    }
}

impl From<PartitionError> for ScServerError {
    fn from(error: PartitionError) -> Self {
        Self::PartitionError(error)
    }
}
