#[cfg(feature = "file")]
use fluvio_future::zero_copy::SendFileError;
use std::io::Error as IoError;

#[derive(thiserror::Error, Debug)]
pub enum SocketError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("Socket closed")]
    SocketClosed,
    #[cfg(feature = "file")]
    #[error("Zero-copy IO error")]
    SendFile(#[from] SendFileError),
}
