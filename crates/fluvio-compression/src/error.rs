#[cfg(feature = "compress")]
use bytes::buf::Writer;

#[cfg(feature = "compress")]
use bytes::BytesMut;

#[cfg(feature = "compress")]
use snap::write::{IntoInnerError, FrameEncoder};

use fluvio_types::compression::CompressionParseError;

#[derive(thiserror::Error, Debug)]
pub enum CompressionError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Failed to parse compression type: {0}")]
    ParseError(#[from] CompressionParseError),
    #[error("Unreachable error")]
    UnreachableError,
    #[error("error flushing Snap encoder: {0}")]
    #[cfg(feature = "compress")]
    SnapError(#[from] Box<IntoInnerError<FrameEncoder<Writer<BytesMut>>>>),
    #[error("error flushing Snap encoder: {0}")]
    #[cfg(feature = "compress")]
    Lz4Error(#[from] lz4_flex::frame::Error),
}
