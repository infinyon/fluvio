use std::io::{Read, Write};

use crate::error::CompressionError;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

pub fn compress(src: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let buf = Vec::with_capacity(src.len());
    let mut encoder = FrameEncoder::new(buf);
    encoder.write_all(src)?;
    Ok(encoder.finish()?)
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut buffer: Vec<u8> = Vec::new();
    FrameDecoder::new(src).read_to_end(&mut buffer)?;

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let text = "FLUVIO_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let compressed = compress(text.as_bytes()).unwrap();

        assert!(compressed.len() < text.as_bytes().len());

        let uncompressed = String::from_utf8(uncompress(compressed.as_slice()).unwrap()).unwrap();

        assert_eq!(uncompressed, text);
    }
}
