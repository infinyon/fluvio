use fluvio_future::zero_copy::SendFileError;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlvSocketError {
    #[error("IO error: {source}")]
    IoError {
        #[from]
        source: IoError,
    },
    #[error("Zero-copy IO error: {source}")]
    SendFileError {
        #[from]
        source: SendFileError,
    },
}
