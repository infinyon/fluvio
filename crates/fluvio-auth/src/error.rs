use std::io::Error as IoError;
use thiserror::Error;

/// Possible errors from Auth
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("IoError")]
    IoError(#[from] IoError),
}

impl From<AuthError> for IoError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::IoError(source) => source,
        }
    }
}
