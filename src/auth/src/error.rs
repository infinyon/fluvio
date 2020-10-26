use std::io::Error as IoError;
use thiserror::Error;

/// Possible errors from Auth
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("IoError")]
    IoError {
        #[from]
        source: IoError,
    },
}

impl Into<IoError> for AuthError {
    fn into(self) -> IoError {
        match self {
            Self::IoError { source } => source,
        }
    }
}
