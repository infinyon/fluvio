use std::str::FromStr;

#[cfg(feature = "compress")]
use bytes::Bytes;

mod error;

#[cfg(feature = "compress")]
mod gzip;

#[cfg(feature = "compress")]
mod snappy;

#[cfg(feature = "compress")]
mod lz4;

#[cfg(feature = "compress")]
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
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl TryFrom<i8> for Compression {
    type Error = CompressionError;
    fn try_from(v: i8) -> Result<Self, CompressionError> {
        match v {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Gzip),
            2 => Ok(Compression::Snappy),
            3 => Ok(Compression::Lz4),
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
            "gzip" => Ok(Compression::Gzip),
            "snappy" => Ok(Compression::Snappy),
            "lz4" => Ok(Compression::Lz4),
            "zstd" => Ok(Compression::Zstd),
            _ => Err(CompressionError::UnknownCompressionFormat(s.into())),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Snappy => write!(f, "snappy"),
            Compression::Lz4 => write!(f, "lz4"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

#[cfg(feature = "compress")]
impl Compression {
    /// Compress the given data, returning the compressed data
    pub fn compress(&self, src: &[u8]) -> Result<Bytes, CompressionError> {
        match *self {
            Compression::None => Ok(Bytes::copy_from_slice(src)),
            Compression::Gzip => gzip::compress(src),
            Compression::Snappy => snappy::compress(src),
            Compression::Lz4 => lz4::compress(src),
            Compression::Zstd => zstd::compress(src),
        }
    }

    /// Uncompresss the given data, returning the uncompressed data if any compression was applied, otherwise returns None
    pub fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        match *self {
            Compression::None => Ok(None),
            Compression::Gzip => {
                let output = gzip::uncompress(src)?;
                Ok(Some(output))
            }
            Compression::Snappy => {
                let output = snappy::uncompress(src)?;
                Ok(Some(output))
            }
            Compression::Lz4 => {
                let output = lz4::uncompress(src)?;
                Ok(Some(output))
            }
            Compression::Zstd => {
                let output = zstd::uncompress(src)?;
                Ok(Some(output))
            }
        }
    }
}
