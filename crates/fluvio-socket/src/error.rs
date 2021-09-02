use std::io::Error as IoError;

#[derive(thiserror::Error, Debug)]
pub enum SocketError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("Socket closed")]
    SocketClosed,
}
