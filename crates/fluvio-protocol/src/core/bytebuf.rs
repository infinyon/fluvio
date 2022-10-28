use std::io::Error;
use std::io::ErrorKind;

use bytes::buf::UninitSlice;
use bytes::{Buf, Bytes, BufMut};

use crate::{Encoder, Decoder, Version};

/// Represnts a SmartModule WASM File bytes.
///
/// Provides a `Encoder` and `Decoder` implementation optimized for WASM files
/// used in SmartModules.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ByteBuf {
    inner: Vec<u8>,
}

impl ByteBuf {
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn inner(&self) -> Vec<u8> {
        self.inner.clone()
    }
}

impl From<Vec<u8>> for ByteBuf {
    fn from(bytes: Vec<u8>) -> Self {
        ByteBuf { inner: bytes }
    }
}

impl Decoder for ByteBuf {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: i32 = 0;
        len.decode(src, version)?;

        if len < 1 {
            return Ok(());
        }

        for _ in 0..len {
            let mut value = 0_u8;
            value.decode(src, version)?;
            self.inner.push(value);
        }

        Ok(())
    }
}

impl Encoder for ByteBuf {
    fn write_size(&self, version: Version) -> usize {
        self.inner
            .iter()
            .fold(4, |sum, val| sum + val.write_size(version))
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
                    "Not enough capacity for ByteBuf. Expected: {}, Remaining: {}",
                    expected, remaining
                ),
            ));
        }

        dest.put_u32(self.inner.len() as u32);
        dest.put_slice(self.inner.as_slice());

        Ok(())
    }

    fn as_bytes(&self, _version: Version) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(self.inner.as_slice()))
    }
}

unsafe impl BufMut for ByteBuf {
    #[inline]
    fn remaining_mut(&self) -> usize {
        self.inner.remaining_mut()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.inner.advance_mut(cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.inner.chunk_mut()
    }

    fn put<T: Buf>(&mut self, src: T)
    where
        Self: Sized,
    {
        self.inner.put(src);
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.inner.put_slice(src);
    }

    fn put_bytes(&mut self, val: u8, cnt: usize) {
        self.inner.put_bytes(val, cnt);
    }
}

#[cfg(feature = "use_serde")]
mod base64 {
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    use crate::ByteBuf;

    impl Serialize for ByteBuf {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let base64 = base64::encode(&self.inner);
            String::serialize(&base64, serializer)
        }
    }

    impl<'de> Deserialize<'de> for ByteBuf {
        fn deserialize<D>(d: D) -> Result<ByteBuf, D::Error>
        where
            D: Deserializer<'de>,
        {
            let b64 = String::deserialize(d)?;
            let bytes: Vec<u8> =
                base64::decode(b64.as_bytes()).map_err(serde::de::Error::custom)?;
            let bytebuf = ByteBuf::from(bytes);

            Ok(bytebuf)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{Decoder, Encoder, DecoderVarInt};
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
    fn test_encodes_as_vec_u8() {
        let raw_data: [u8; 10] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];

        // Data encoded for Vec<u8> will have the first 4 bytes (32 bits) for
        // data length, and the remaining bytes represents the actual data
        let encoded_expect: [u8; 14] = [
            0x00, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
        ];

        let mut encoded_data: Vec<u8> = Vec::default();
        let encoded_res = Vec::<u8>::from(raw_data).encode(&mut encoded_data, 0);

        assert!(encoded_res.is_ok());
        assert_eq!(
            encoded_expect,
            encoded_data.as_slice(),
            "encoded version doesn't match with expected"
        );
        assert_eq!(encoded_data[3], 0x0A, "the length value doesnt match");

        let mut bytebuf_encoded = ByteBuf::default();
        let encode_bytebuf_res = ByteBuf::from(Vec::from(raw_data)).encode(&mut bytebuf_encoded, 0);

        assert!(encode_bytebuf_res.is_ok());
        assert_eq!(
            bytebuf_encoded.inner,
            encoded_expect.as_slice(),
            "encoded version doesn't match with expected"
        );
        assert_eq!(
            bytebuf_encoded.inner,
            encoded_data.as_slice(),
            "encoded version doesn't match with expected"
        );
        assert_eq!(
            bytebuf_encoded.inner[3], 0x0A,
            "the length value doesnt match"
        );
    }

    #[test]
    fn test_decodes_as_bytebuf() {
        let raw_data: [u8; 10] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];
        let encoded_expect: [u8; 14] = [
            0x00, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
        ];
        let mut encoded_data: Vec<u8> = Vec::default();
        let encoded_res = Vec::<u8>::from(raw_data).encode(&mut encoded_data, 0);
        let mut bytebuf_encoded = ByteBuf::default();
        let encode_bytebuf_res = ByteBuf::from(Vec::from(raw_data)).encode(&mut bytebuf_encoded, 0);

        assert_eq!(
            encoded_data.as_slice(),
            encoded_expect,
            "the encoded output doesn't match with the expected for Vec<u8>"
        );
        assert_eq!(
            bytebuf_encoded.inner.as_slice(),
            encoded_expect,
            "the encoded output doesn't match with the expected for Vec<u8>"
        );

        let mut decoded_vecu8: Vec<u8> = Vec::default();
        let decoded_vecu8_res = decoded_vecu8.decode(&mut Cursor::new(&encoded_expect), 0);
        println!("hello: {:?}", raw_data);

        assert!(decoded_vecu8_res.is_ok());
        assert_eq!(decoded_vecu8.len(), 10);

        let mut decoded_bytebuf: ByteBuf = ByteBuf::default();
        let decoded_bytebuf_res = decoded_bytebuf.decode(&mut Cursor::new(&encoded_expect), 0);

        assert!(decoded_bytebuf_res.is_ok());
        assert_eq!(decoded_bytebuf.inner.len(), 10);

        assert_eq!(decoded_bytebuf.inner, decoded_vecu8);
    }
}
