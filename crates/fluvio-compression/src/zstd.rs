use std::io::{Read, Write};

use bytes::{BufMut, Bytes, BytesMut};
use zstd::{Decoder, Encoder};

use crate::error::CompressionError;

pub fn compress(src: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = Encoder::new(BytesMut::new().writer(), 1)?;
    encoder.write_all(src)?;
    Ok(encoder.finish()?.into_inner().freeze())
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut decoder = Decoder::new(src)?;
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

        assert!(compressed.len() < text.len());

        let uncompressed = String::from_utf8(uncompress(compressed.reader()).unwrap()).unwrap();

        assert_eq!(uncompressed, text);
    }
}
