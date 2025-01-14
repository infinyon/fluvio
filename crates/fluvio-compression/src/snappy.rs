use std::io::{Read, Write};
use bytes::{BufMut, Bytes, BytesMut};

use crate::error::CompressionError;
use snap::{read::FrameDecoder, write::FrameEncoder};

pub fn compress(src: &[u8]) -> Result<Bytes, CompressionError> {
    let buf = BytesMut::with_capacity(src.len());
    let mut encoder = FrameEncoder::new(buf.writer());
    encoder.write_all(src)?;
    Ok(encoder
        .into_inner()
        .map(|w| w.into_inner().freeze())
        .map_err(Box::new)?)
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut buffer: Vec<u8> = Vec::new();
    FrameDecoder::new(src).read_to_end(&mut buffer)?;

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
