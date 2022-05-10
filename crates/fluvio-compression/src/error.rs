use snap::write::{IntoInnerError, FrameEncoder};

#[derive(thiserror::Error, Debug)]
pub enum CompressionError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("unknown compression format: {0}")]
    UnknownCompressionFormat(String),
    #[error("unknown GZIP compression level: {0}")]
    UnknownGzipLevel(String),
    #[error("error flushing Snap encoder: {0}")]
    SnapError(#[from] Box<IntoInnerError<FrameEncoder<Vec<u8>>>>),
    #[error("error flushing Snap encoder: {0}")]
    Lz4Error(#[from] lz4_flex::frame::Error),
    #[error("Unreachable error")]
    UnreachableError,
}
