use std::io::Error as IoError;

#[derive(thiserror::Error, Debug)]
pub enum SocketError {
    #[error("Socket io {msg}")]
    Io { source: IoError, msg: String },
    #[error("Socket closed")]
    SocketClosed,
    #[error("Socket is stale")]
    SocketStale,
}

impl From<IoError> for SocketError {
    fn from(err: IoError) -> Self {
        let msg = err.to_string();
        SocketError::Io { source: err, msg }
    }
}
