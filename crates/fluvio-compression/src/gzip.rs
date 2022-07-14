use std::io::{Read, Write};
use bytes::{BufMut, Bytes, BytesMut};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::CompressionError;

pub fn compress(src: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = GzEncoder::new(BytesMut::new().writer(), Compression::default());
    encoder.write_all(src)?;
    Ok(encoder.finish()?.into_inner().freeze())
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut decoder = GzDecoder::new(src);
    let mut buffer: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let text = "FLUVIO_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let compressed = compress(text.as_bytes()).unwrap();

        assert!(compressed.len() < text.as_bytes().len());

        let uncompressed = String::from_utf8(uncompress(compressed.reader()).unwrap()).unwrap();

        assert_eq!(uncompressed, text);
    }
}
