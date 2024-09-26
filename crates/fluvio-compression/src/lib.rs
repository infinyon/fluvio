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
    #[cfg(feature = "gzip")]
    Gzip = 1,
    #[cfg(feature = "snap")]
    Snappy = 2,
    #[cfg(feature = "lz4")]
    Lz4 = 3,
    #[cfg(feature = "zstd")]
    Zstd = 4,
}

impl TryFrom<i8> for Compression {
    type Error = CompressionError;
    fn try_from(v: i8) -> Result<Self, CompressionError> {
        match v {
            0 => Ok(Compression::None),
            #[cfg(feature = "gzip")]
            1 => Ok(Compression::Gzip),
            #[cfg(feature = "snap")]
            2 => Ok(Compression::Snappy),
            #[cfg(feature = "lz4")]
            3 => Ok(Compression::Lz4),
            #[cfg(feature = "zstd")]
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
            #[cfg(feature = "gzip")]
            "gzip" => Ok(Compression::Gzip),
            #[cfg(feature = "snap")]
            "snappy" => Ok(Compression::Snappy),
            #[cfg(feature = "lz4")]
            "lz4" => Ok(Compression::Lz4),
            #[cfg(feature = "zstd")]
            "zstd" => Ok(Compression::Zstd),
            _ => Err(CompressionError::UnknownCompressionFormat(s.into())),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Compression::None => write!(f, "none"),
            #[cfg(feature = "gzip")]
            Compression::Gzip => write!(f, "gzip"),
            #[cfg(feature = "snap")]
            Compression::Snappy => write!(f, "snappy"),
            #[cfg(feature = "lz4")]
            Compression::Lz4 => write!(f, "lz4"),
            #[cfg(feature = "zstd")]
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

impl Compression {
    /// Compress the given data, returning the compressed data
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

#[cfg(any(feature = "gzip", feature = "snap", feature = "lz4", feature = "zstd"))]
impl From<fluvio_types::compression::Compression> for Compression {
    fn from(fcc: fluvio_types::compression::Compression) -> Self {
        use fluvio_types::compression::Compression as CompressionType;

        match fcc {
            CompressionType::None => Compression::None,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => Compression::Gzip,
            #[cfg(feature = "snap")]
            CompressionType::Snappy => Compression::Snappy,
            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => Compression::Lz4,
            #[cfg(feature = "zstd")]
            CompressionType::Zstd => Compression::Zstd,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Compression;

    #[test]
    fn converts_from_fluvio_compression() {
        use fluvio_types::compression::Compression as CompressionType;

        assert_eq!(Compression::from(CompressionType::None), Compression::None);

        #[cfg(feature = "gzip")]
        assert_eq!(Compression::from(CompressionType::Gzip), Compression::Gzip);
        #[cfg(feature = "snap")]
        assert_eq!(
            Compression::from(CompressionType::Snappy),
            Compression::Snappy
        );

        #[cfg(feature = "lz4")]
        assert_eq!(Compression::from(CompressionType::Lz4), Compression::Lz4);

        #[cfg(feature = "zstd")]
        assert_eq!(Compression::from(CompressionType::Zstd), Compression::Zstd);
    }
}
