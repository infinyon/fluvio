#[cfg(feature = "bytes")]
use bytes::buf::Writer;

#[cfg(feature = "bytes")]
use bytes::BytesMut;

#[cfg(feature = "bytes")]
use snap::write::{IntoInnerError, FrameEncoder};

#[derive(thiserror::Error, Debug)]
pub enum CompressionError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Unreachable error")]
    UnreachableError,
    #[error("unknown compression format: {0}")]
    UnknownCompressionFormat(String),
    #[error("error flushing Snap encoder: {0}")]
    #[cfg(feature = "bytes")]
    SnapError(#[from] Box<IntoInnerError<FrameEncoder<Writer<BytesMut>>>>),
    #[error("error flushing Snap encoder: {0}")]
    #[cfg(feature = "bytes")]
    Lz4Error(#[from] lz4_flex::frame::Error),
}
