#[cfg(feature = "compress")]
use bytes::buf::Writer;

#[cfg(feature = "compress")]
use bytes::BytesMut;

#[cfg(feature = "compress")]
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
    #[cfg(feature = "compress")]
    SnapError(#[from] Box<IntoInnerError<FrameEncoder<Writer<BytesMut>>>>),
    #[error("error flushing Snap encoder: {0}")]
    #[cfg(feature = "compress")]
    Lz4Error(#[from] lz4_flex::frame::Error),
}
