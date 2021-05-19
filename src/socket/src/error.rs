#[cfg(feature = "file")]
use fluvio_future::zero_copy::SendFileError;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlvSocketError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Socket closed")]
    SocketClosed,
    #[cfg(feature = "file")]
    #[error("Zero-copy IO error")]
    SendFileError(#[from] SendFileError),
}
