use std::str::FromStr;

mod error;

use bytes::Bytes;

#[cfg(feature = "gzip")]
mod gzip;

#[cfg(feature = "snap")]
mod snappy;

#[cfg(feature = "lz4")]
mod lz4;

#[cfg(feature = "zstd")]
mod zstd;

pub use error::CompressionError;
use serde::{Serialize, Deserialize};

/// The compression algorithm used to compress and decompress records in fluvio batches
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
#[derive(Default)]
pub enum Compression {
    #[default]
    None = 0,
    #[cfg(any(feature = "types", feature = "gzip"))]
    Gzip = 1,
    #[cfg(any(feature = "types", feature = "snap"))]
    Snappy = 2,
    #[cfg(any(feature = "types", feature = "lz4"))]
    Lz4 = 3,
    #[cfg(any(feature = "types", feature = "zstd"))]
    Zstd = 4,
}

impl TryFrom<i8> for Compression {
    type Error = CompressionError;
    fn try_from(v: i8) -> Result<Self, CompressionError> {
        match v {
            0 => Ok(Compression::None),
            #[cfg(any(feature = "types", feature = "gzip"))]
            1 => Ok(Compression::Gzip),
            #[cfg(any(feature = "types", feature = "snap"))]
            2 => Ok(Compression::Snappy),
            #[cfg(any(feature = "types", feature = "lz4"))]
            3 => Ok(Compression::Lz4),
            #[cfg(any(feature = "types", feature = "zstd"))]
            4 => Ok(Compression::Zstd),
            _ => Err(CompressionError::UnknownCompressionFormat(format!(
                "i8 representation: {v}"
            ))),
        }
    }
}

impl FromStr for Compression {
    type Err = CompressionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Compression::None),
            #[cfg(any(feature = "types", feature = "gzip"))]
            "gzip" => Ok(Compression::Gzip),
            #[cfg(any(feature = "types", feature = "snap"))]
            "snappy" => Ok(Compression::Snappy),
            #[cfg(any(feature = "types", feature = "lz4"))]
            "lz4" => Ok(Compression::Lz4),
            #[cfg(any(feature = "types", feature = "zstd"))]
            "zstd" => Ok(Compression::Zstd),
            _ => Err(CompressionError::UnknownCompressionFormat(s.into())),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Compression::None => write!(f, "none"),
            #[cfg(any(feature = "types", feature = "gzip"))]
            Compression::Gzip => write!(f, "gzip"),
            #[cfg(any(feature = "types", feature = "snap"))]
            Compression::Snappy => write!(f, "snappy"),
            #[cfg(any(feature = "types", feature = "lz4"))]
            Compression::Lz4 => write!(f, "lz4"),
            #[cfg(any(feature = "types", feature = "zstd"))]
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

impl Compression {
    /// Compress the given data, returning the compressed data
    #[cfg(feature = "compress")]
    pub fn compress(&self, src: &[u8]) -> Result<Bytes, CompressionError> {
        match *self {
            Compression::None => Ok(Bytes::copy_from_slice(src)),
            #[cfg(feature = "gzip")]
            Compression::Gzip => gzip::compress(src),
            #[cfg(feature = "snap")]
            Compression::Snappy => snappy::compress(src),
            #[cfg(feature = "lz4")]
            Compression::Lz4 => lz4::compress(src),
            #[cfg(feature = "zstd")]
            Compression::Zstd => zstd::compress(src),
        }
    }

    /// Uncompresss the given data, returning the uncompressed data if any compression was applied, otherwise returns None
    #[allow(unused_variables)]
    #[cfg(feature = "compress")]
    pub fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        match *self {
            Compression::None => Ok(None),
            #[cfg(feature = "gzip")]
            Compression::Gzip => {
                let output = gzip::uncompress(src)?;
                Ok(Some(output))
            }
            #[cfg(feature = "snap")]
            Compression::Snappy => {
                let output = snappy::uncompress(src)?;
                Ok(Some(output))
            }
            #[cfg(feature = "lz4")]
            Compression::Lz4 => {
                let output = lz4::uncompress(src)?;
                Ok(Some(output))
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let output = zstd::uncompress(src)?;
                Ok(Some(output))
            }
        }
    }
}
