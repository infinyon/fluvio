use std::str::FromStr;

use serde::{Serialize, Deserialize};

#[derive(thiserror::Error, Debug)]
pub enum CompressionParseError {
    #[error("unknown compression format: {0}")]
    UnknownCompressionFormat(String),
}

pub const COMPRESSION_CODE_NONE: i8 = 0;
pub const COMPRESSION_CODE_GZIP: i8 = 1;
pub const COMPRESSION_CODE_SNAPPY: i8 = 2;
pub const COMPRESSION_CODE_LZ4: i8 = 3;
pub const COMPRESSION_CODE_ZSTD: i8 = 4;

pub const COMPRESSION_NONE: &str = "none";
pub const COMPRESSION_GZIP: &str = "gzip";
pub const COMPRESSION_SNAPPY: &str = "snappy";
pub const COMPRESSION_LZ4: &str = "lz4";
pub const COMPRESSION_ZSTD: &str = "zstd";

/// The compression algorithm used to compress and decompress records in fluvio batches
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
#[derive(Default)]
pub enum Compression {
    #[default]
    None = COMPRESSION_CODE_NONE,
    Gzip = COMPRESSION_CODE_GZIP,
    Snappy = COMPRESSION_CODE_SNAPPY,
    Lz4 = COMPRESSION_CODE_LZ4,
    Zstd = COMPRESSION_CODE_ZSTD,
}

impl TryFrom<i8> for Compression {
    type Error = CompressionParseError;

    fn try_from(v: i8) -> Result<Self, CompressionParseError> {
        match v {
            COMPRESSION_CODE_NONE => Ok(Compression::None),
            COMPRESSION_CODE_GZIP => Ok(Compression::Gzip),
            COMPRESSION_CODE_SNAPPY => Ok(Compression::Snappy),
            COMPRESSION_CODE_LZ4 => Ok(Compression::Lz4),
            COMPRESSION_CODE_ZSTD => Ok(Compression::Zstd),
            _ => Err(CompressionParseError::UnknownCompressionFormat(format!(
                "i8 representation: {v}"
            ))),
        }
    }
}

impl FromStr for Compression {
    type Err = CompressionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            COMPRESSION_NONE => Ok(Compression::None),
            COMPRESSION_GZIP => Ok(Compression::Gzip),
            COMPRESSION_SNAPPY => Ok(Compression::Snappy),
            COMPRESSION_LZ4 => Ok(Compression::Lz4),
            COMPRESSION_ZSTD => Ok(Compression::Zstd),
            _ => Err(CompressionParseError::UnknownCompressionFormat(s.into())),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Compression::None => write!(f, "{}", COMPRESSION_NONE),
            Compression::Gzip => write!(f, "{}", COMPRESSION_GZIP),
            Compression::Snappy => write!(f, "{}", COMPRESSION_SNAPPY),
            Compression::Lz4 => write!(f, "{}", COMPRESSION_LZ4),
            Compression::Zstd => write!(f, "{}", COMPRESSION_ZSTD),
        }
    }
}
