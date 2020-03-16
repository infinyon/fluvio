use std::fmt;
use std::io::Error as IoError;
use flv_future_aio::zero_copy::SendFileError;

#[derive(Debug)]
pub enum KfSocketError {
    IoError(IoError),
    SendFileError(SendFileError),
}

impl From<IoError> for KfSocketError {
    fn from(error: IoError) -> Self {
        KfSocketError::IoError(error)
    }
}

impl From<SendFileError> for KfSocketError {
    fn from(error: SendFileError) -> Self {
        KfSocketError::SendFileError(error)
    }
}

impl fmt::Display for KfSocketError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KfSocketError::IoError(err) => write!(f, "{}", err),
            KfSocketError::SendFileError(err) => write!(f, "{:#?}", err),
        }
    }
}
