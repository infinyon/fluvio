use std::str::FromStr;

mod error;

mod gzip;
mod snappy;
mod lz4;

pub use error::CompressionError;
use serde::{Serialize, Deserialize};

/// The compression algorithm used to compress and decompress records in fluvio batches
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum Compression {
    None,
    Gzip(GzipLevel),
    Snappy,
    Lz4,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

impl TryFrom<u8> for Compression {
    type Error = CompressionError;
    fn try_from(v: u8) -> Result<Self, CompressionError> {
        match v {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Gzip(GzipLevel::default())),
            2 => Ok(Compression::Snappy),
            3 => Ok(Compression::Lz4),
            _ => Err(CompressionError::UnknownCompressionFormat(format!(
                "u8 representation: {}",
                v
            ))),
        }
    }
}

impl From<Compression> for u8 {
    fn from(compression: Compression) -> Self {
        use Compression::*;
        match compression {
            None => 0,
            Gzip(_) => 1,
            Snappy => 2,
            Lz4 => 3,
        }
    }
}

impl FromStr for Compression {
    type Err = CompressionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip(GzipLevel::default())),
            "snappy" => Ok(Compression::Snappy),
            "lz4" => Ok(Compression::Lz4),
            _ => Err(CompressionError::UnknownCompressionFormat(s.into())),
        }
    }
}

impl Compression {
    /// Compress the given data, returning the compressed data
    pub fn compress(&self, src: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match *self {
            Compression::None => Ok(src.to_vec()),
            Compression::Gzip(level) => gzip::compress(src, level),
            Compression::Snappy => snappy::compress(src),
            Compression::Lz4 => lz4::compress(src),
        }
    }

    /// Uncompresss the given data, returning the uncompressed data if any compression was applied, otherwise returns None
    pub fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        match *self {
            Compression::None => Ok(None),
            Compression::Gzip(_) => {
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
        }
    }
}
impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Compression::None => write!(f, "none"),
            Compression::Gzip(_) => write!(f, "gzip"),
            Compression::Snappy => write!(f, "snappy"),
            Compression::Lz4 => write!(f, "lz4"),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum GzipLevel {
    Level1 = 1,
    Level2 = 2,
    Level3 = 3,
    Level4 = 4,
    Level5 = 5,
    Level6 = 6,
    Level7 = 7,
    Level8 = 8,
    Level9 = 9,
}

impl FromStr for GzipLevel {
    type Err = CompressionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use GzipLevel::*;
        match s {
            "1" => Ok(Level1),
            "2" => Ok(Level2),
            "3" => Ok(Level3),
            "4" => Ok(Level4),
            "5" => Ok(Level5),
            "6" => Ok(Level6),
            "7" => Ok(Level7),
            "8" => Ok(Level8),
            "9" => Ok(Level9),
            _ => Err(CompressionError::UnknownGzipLevel(s.into())),
        }
    }
}

impl Default for GzipLevel {
    fn default() -> Self {
        GzipLevel::Level6
    }
}

impl TryFrom<i8> for GzipLevel {
    type Error = CompressionError;

    fn try_from(v: i8) -> Result<Self, CompressionError> {
        use GzipLevel::*;
        match v {
            1 => Ok(Level1),
            2 => Ok(Level2),
            3 => Ok(Level3),
            4 => Ok(Level4),
            5 => Ok(Level5),
            6 => Ok(Level6),
            7 => Ok(Level7),
            8 => Ok(Level8),
            9 => Ok(Level9),
            _ => Err(CompressionError::UnknownCompressionFormat(format!(
                "i8 representation: {}",
                v
            ))),
        }
    }
}

impl TryFrom<GzipLevel> for flate2::Compression {
    type Error = CompressionError;

    fn try_from(level: GzipLevel) -> Result<Self, Self::Error> {
        let int_level = level as u32;
        match int_level {
            int_level if int_level == 0 => Ok(flate2::Compression::default()),
            int_level if int_level >= 1 && int_level <= 9 => {
                Ok(flate2::Compression::new(int_level))
            }
            _ => Err(CompressionError::UnknownGzipLevel(format!(
                "Gzip supports compression levels 1..9, supplied level: {int_level}"
            ))),
        }
    }
}

impl std::fmt::Display for GzipLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}
