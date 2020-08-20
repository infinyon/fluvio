// error.rs
//  Server Error handling (union of errors used by server)
//

use std::fmt;
use std::io::Error as StdIoError;

use futures::channel::mpsc::SendError;

use k8_client::ClientError;
use fluvio_types::PartitionError;
use fluvio_types::SpuId;

#[derive(Debug)]
pub enum ScK8Error {
    IoError(StdIoError),
    ClientError(ClientError),
    SendError(SendError),
    PartitionError(PartitionError),
    UnknownSpu(SpuId),
}

impl From<StdIoError> for ScK8Error {
    fn from(error: StdIoError) -> Self {
        Self::IoError(error)
    }
}

impl From<SendError> for ScK8Error {
    fn from(error: SendError) -> Self {
        Self::SendError(error)
    }
}

impl From<PartitionError> for ScK8Error {
    fn from(error: PartitionError) -> Self {
        Self::PartitionError(error)
    }
}

impl From<ClientError> for ScK8Error {
    fn from(error: ClientError) -> Self {
        Self::ClientError(error)
    }
}

impl fmt::Display for ScK8Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::SendError(err) => write!(f, "{}", err),
            Self::PartitionError(err) => write!(f, "{}", err),
            Self::UnknownSpu(spu) => write!(f, "unknown spu: {}", spu),
            Self::ClientError(err) => write!(f, "{}", err),
        }
    }
}
