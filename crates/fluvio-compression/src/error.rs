use bytes::buf::Writer;
use bytes::BytesMut;
use snap::write::{IntoInnerError, FrameEncoder};

#[derive(thiserror::Error, Debug)]
pub enum CompressionError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("unknown compression format: {0}")]
    UnknownCompressionFormat(String),
    #[error("error flushing Snap encoder: {0}")]
    SnapError(#[from] Box<IntoInnerError<FrameEncoder<Writer<BytesMut>>>>),
    #[error("error flushing Snap encoder: {0}")]
    Lz4Error(#[from] lz4_flex::frame::Error),
    #[error("Unreachable error")]
    UnreachableError,
}
