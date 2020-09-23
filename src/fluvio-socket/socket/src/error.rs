use fluvio_future::zero_copy::SendFileError;
use std::fmt;
use std::io::Error as IoError;

#[derive(Debug)]
pub enum FlvSocketError {
    IoError(IoError),
    SendFileError(SendFileError),
}

impl From<IoError> for FlvSocketError {
    fn from(error: IoError) -> Self {
        FlvSocketError::IoError(error)
    }
}

impl From<SendFileError> for FlvSocketError {
    fn from(error: SendFileError) -> Self {
        FlvSocketError::SendFileError(error)
    }
}

impl fmt::Display for FlvSocketError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FlvSocketError::IoError(err) => write!(f, "{}", err),
            FlvSocketError::SendFileError(err) => write!(f, "{:#?}", err),
        }
    }
}
