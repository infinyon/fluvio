use std::io::Error;
use std::io::ErrorKind;
use std::ops::Deref;

use bytes::{Buf, Bytes, BufMut};

use crate::{Encoder, Decoder, Version};

/// Bytes Buffer with an optimized implementation for encoding and decoding.
/// Useful for handling an immutable set of bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ByteBuf {
    inner: Bytes,
}

impl From<Bytes> for ByteBuf {
    fn from(bytes: Bytes) -> Self {
        ByteBuf { inner: bytes }
    }
}

impl From<Vec<u8>> for ByteBuf {
    fn from(bytes: Vec<u8>) -> Self {
        ByteBuf {
            inner: Bytes::from_iter(bytes.into_iter()),
        }
    }
}

impl From<ByteBuf> for Vec<u8> {
    fn from(bytebuf: ByteBuf) -> Self {
        bytebuf.inner.to_vec()
    }
}

impl Deref for ByteBuf {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Decoder for ByteBuf {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: u32 = 0;
        len.decode(src, version)?;

        if len < 1 {
            return Ok(());
        }

        self.inner = src.copy_to_bytes(len as usize);

        Ok(())
    }
}

impl Encoder for ByteBuf {
    fn write_size(&self, version: Version) -> usize {
        0_u32.write_size(version) + self.inner.len()
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        let remaining = dest.remaining_mut();
        let expected = self.write_size(version);

        if remaining < expected {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Not enough capacity for ByteBuf. Expected: {expected}, Remaining: {remaining}"
                ),
            ));
        }

        dest.put_u32(self.inner.len() as u32);
        dest.put(self.inner.clone());

        Ok(())
    }

    fn as_bytes(&self, _version: Version) -> Result<Bytes, Error> {
        Ok(self.inner.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{Decoder, Encoder};
    use super::ByteBuf;

    #[test]
    fn test_is_empty_by_default() {
        let bytebuf = ByteBuf::default();

        assert!(bytebuf.is_empty());
        assert!(bytebuf.inner.is_empty());
    }

    #[test]
    fn test_is_empty_from_inner_vec() {
        let bytebuf = ByteBuf::from(vec![128, 129, 130, 131]);

        assert_eq!(bytebuf.is_empty(), bytebuf.inner.is_empty());
    }

    #[test]
    fn test_is_empty_from_inner_empty_vec() {
        let bytebuf = ByteBuf::from(Vec::default());

        assert_eq!(bytebuf.is_empty(), bytebuf.inner.is_empty());
    }

    #[test]
    fn test_len_from_inner_vec() {
        let bytebuf = ByteBuf::from(vec![128, 129, 130, 131]);

        assert_eq!(bytebuf.len(), bytebuf.inner.len());
    }

    #[test]
    fn test_encode_bytebuf() {
        let mut dest = Vec::default();
        let value: ByteBuf = ByteBuf::from(vec![0x10, 0x11]);
        let result = value.encode(&mut dest, 0);

        assert!(result.is_ok());

        // Length + Contents
        assert_eq!(dest.len(), 6);

        // Length Bytes
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x00);
        assert_eq!(dest[2], 0x00);
        assert_eq!(dest[3], 0x02);

        // Actual Content
        assert_eq!(dest[3], 0x02);
        assert_eq!(dest[5], 0x11);

        // Length in 32bits (4 bytes) + Contents (5 elements)
        assert_eq!(value.write_size(0), dest.len());
    }

    #[test]
    fn test_bytebuf_encodes_transparent_with_vecu8() {
        let raw_data: [u8; 10] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];
        let mut encoded_data: Vec<u8> = Vec::default();

        Vec::<u8>::from(raw_data)
            .encode(&mut encoded_data, 0)
            .expect("Failed to encode Vec<u8>");

        assert_eq!(encoded_data[3], 0x0A, "the length value doesnt match");

        let mut bytebuf_encoded: Vec<u8> = Vec::default();
        let encode_bytebuf_res = ByteBuf::from(Vec::from(raw_data)).encode(&mut bytebuf_encoded, 0);

        assert!(encode_bytebuf_res.is_ok());
        assert_eq!(
            bytebuf_encoded,
            encoded_data.as_slice(),
            "encoded version doesn't match with expected"
        );
        assert_eq!(
            bytebuf_encoded,
            encoded_data.as_slice(),
            "encoded version doesn't match with expected"
        );
        assert_eq!(bytebuf_encoded[3], 0x0A, "the length value doesnt match");
    }

    #[test]
    fn test_bytebuf_decode_transparent_with_vecu8() {
        let raw_data: [u8; 10] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];

        let mut encoded_data: Vec<u8> = Vec::default();
        Vec::<u8>::from(raw_data)
            .encode(&mut encoded_data, 0)
            .expect("Failed to encode Vec<u8>");

        let mut bytebuf_encoded = Vec::default();
        ByteBuf::from(Vec::from(raw_data))
            .encode(&mut bytebuf_encoded, 0)
            .expect("Failed to encode ByteBuf");

        assert_eq!(
            bytebuf_encoded,
            encoded_data.as_slice(),
            "the encoded output doesn't match with the expected for Vec<u8>"
        );

        let mut decoded_vecu8: Vec<u8> = Vec::default();
        decoded_vecu8
            .decode(&mut Cursor::new(&encoded_data.clone()), 0)
            .expect("Failed to decode Vec<u8>");

        assert_eq!(decoded_vecu8.len(), 10);

        let mut decoded_bytebuf: ByteBuf = ByteBuf::default();
        decoded_bytebuf
            .decode(&mut Cursor::new(&encoded_data.clone()), 0)
            .expect("Failed to decode ByteBuf");

        assert_eq!(decoded_bytebuf.inner.len(), 10);
        assert_eq!(decoded_bytebuf.inner, decoded_vecu8);
    }
}
