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
pub use fluvio_types::compression::{Compression, CompressionParseError};

pub trait CompressionExt {
    fn compress(&self, src: &[u8]) -> Result<Bytes, CompressionError>;
    fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError>;
}

impl CompressionExt for Compression {
    fn compress(&self, src: &[u8]) -> Result<Bytes, CompressionError> {
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

    fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
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

// impl Compression {
//     /// Compress the given data, returning the compressed data
//     #[cfg(feature = "compress")]
//     pub fn compress(&self, src: &[u8]) -> Result<Bytes, CompressionError> {
//         match *self {
//             Compression::None => Ok(Bytes::copy_from_slice(src)),
//             #[cfg(feature = "gzip")]
//             Compression::Gzip => gzip::compress(src),
//             #[cfg(feature = "snap")]
//             Compression::Snappy => snappy::compress(src),
//             #[cfg(feature = "lz4")]
//             Compression::Lz4 => lz4::compress(src),
//             #[cfg(feature = "zstd")]
//             Compression::Zstd => zstd::compress(src),
//         }
//     }

//     /// Uncompresss the given data, returning the uncompressed data if any compression was applied, otherwise returns None
//     #[allow(unused_variables)]
//     #[cfg(feature = "compress")]
//     pub fn uncompress(&self, src: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
//         match *self {
//             Compression::None => Ok(None),
//             #[cfg(feature = "gzip")]
//             Compression::Gzip => {
//                 let output = gzip::uncompress(src)?;
//                 Ok(Some(output))
//             }
//             #[cfg(feature = "snap")]
//             Compression::Snappy => {
//                 let output = snappy::uncompress(src)?;
//                 Ok(Some(output))
//             }
//             #[cfg(feature = "lz4")]
//             Compression::Lz4 => {
//                 let output = lz4::uncompress(src)?;
//                 Ok(Some(output))
//             }
//             #[cfg(feature = "zstd")]
//             Compression::Zstd => {
//                 let output = zstd::uncompress(src)?;
//                 Ok(Some(output))
//             }
//         }
//     }
// }
