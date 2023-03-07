use std::io::Error as IoError;

#[derive(thiserror::Error, Debug)]
pub enum SocketError {
    #[error("socket {msg}")]
    Io { source: IoError, msg: String },
    #[error("Socket closed")]
    SocketClosed,
    #[error("Socket is stale")]
    SocketStale,
}

impl From<IoError> for SocketError {
    fn from(err: IoError) -> Self {
        SocketError::Io {
            source: err,
            msg: "".to_string(),
        }
    }
}
