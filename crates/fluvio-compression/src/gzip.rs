use std::io::{Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::GzipLevel;
use crate::error::CompressionError;

pub fn compress(src: &[u8], level: GzipLevel) -> Result<Vec<u8>, CompressionError> {
    let flate2_level: flate2::Compression = level.into();
    let mut encoder = GzEncoder::new(Vec::new(), flate2_level);
    encoder.write_all(src)?;
    Ok(encoder.finish()?)
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut decoder = GzDecoder::new(src);
    let mut buffer: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let text = "FLUVIO_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let compressed = compress(text.as_bytes(), GzipLevel::default()).unwrap();

        assert!(compressed.len() < text.as_bytes().len());

        let uncompressed = String::from_utf8(uncompress(compressed.as_slice()).unwrap()).unwrap();

        assert_eq!(uncompressed, text);
    }

    #[test]
    fn test_compression_level1() {
        let text = "FLUVIO_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let compressed = compress(text.as_bytes(), GzipLevel::Level1).unwrap();
        assert!(compressed.len() < text.as_bytes().len());
        let uncompressed = String::from_utf8(uncompress(compressed.as_slice()).unwrap()).unwrap();
        assert_eq!(uncompressed, text);
    }

    #[test]
    fn test_compression_level9() {
        let text = "FLUVIO_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let compressed = compress(text.as_bytes(), GzipLevel::Level9).unwrap();
        assert!(compressed.len() < text.as_bytes().len());
        let uncompressed = String::from_utf8(uncompress(compressed.as_slice()).unwrap()).unwrap();
        assert_eq!(uncompressed, text);
    }
}
