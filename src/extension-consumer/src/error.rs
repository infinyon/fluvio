use std::io::Error as IoError;

use fluvio::FluvioError;

use crate::common::output::OutputError;

pub type Result<T> = std::result::Result<T, ConsumerError>;

#[derive(thiserror::Error, Debug)]
pub enum ConsumerError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error(transparent)]
    OutputError(#[from] OutputError),
    #[error("Fluvio client error")]
    ClientError(#[from] FluvioError),
    #[error("Invalid argument: {0}")]
    InvalidArg(String),
    #[error("Error finding executable")]
    WhichError(#[from] which::Error),
    #[error("Unknown error: {0}")]
    Other(String),
}

impl ConsumerError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }
}
