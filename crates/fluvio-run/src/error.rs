use std::io::Error as IoError;

pub type Result<T> = std::result::Result<T, RunnerError>;

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Unknown error: {0}")]
    Other(String),
}
