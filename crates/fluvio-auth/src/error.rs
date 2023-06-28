use std::io::Error as IoError;
use thiserror::Error;

/// Possible errors from Auth
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("fluvio-auth error: {0}")]
    IoError(#[from] IoError),
}

impl From<AuthError> for IoError {
    fn from(e: AuthError) -> Self {
        match &e {
            AuthError::IoError(source) => IoError::new(source.kind(), e),
        }
    }
}
