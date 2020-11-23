use std::io::Error as IoError;

use fluvio_extension_common::output::OutputError;

pub type Result<T> = std::result::Result<T, RunnerError>;

#[derive(thiserror::Error, Debug)]
pub enum RunnerError {
    #[error(transparent)]
    IoError {
        #[from]
        source: IoError,
    },
    #[error(transparent)]
    OutputError {
        #[from]
        source: OutputError,
    },

    #[error("Unknown error: {0}")]
    Other(String),
}
