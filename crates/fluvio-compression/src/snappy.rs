use std::io::{Read, Write};

use crate::error::CompressionError;
use snap::{read::FrameDecoder, write::FrameEncoder};

pub fn compress(src: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let buf = Vec::with_capacity(src.len());
    let mut encoder = FrameEncoder::new(buf);
    encoder.write_all(src)?;
    Ok(encoder.into_inner().map_err(Box::new)?)
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>, CompressionError> {
    let mut buffer: Vec<u8> = Vec::new();
    FrameDecoder::new(src).read_to_end(&mut buffer)?;

    Ok(buffer)
}
