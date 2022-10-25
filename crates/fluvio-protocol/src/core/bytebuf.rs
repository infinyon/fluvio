use std::io::Error;
use std::io::ErrorKind;

use bytes::{Buf, Bytes, BufMut};

use crate::{Encoder, Decoder, Version};
use crate::DecoderVarInt;

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
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        if len < 1 {
            return Ok(());
        }

        let mut buf = src.take(len as usize);

        self.inner
            .extend_from_slice(&buf.copy_to_bytes(len as usize));

        if self.len() != len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "varint: ByteBuf, expecting {} but received: {}",
                    len,
                    self.len()
                ),
            ));
        }

        Ok(())
    }
}

impl Encoder for ByteBuf {
    fn write_size(&self, _version: Version) -> usize {
        self.inner.len() + 4
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
        let value: ByteBuf = ByteBuf::from(vec![12, 128, 255, 78, 9]);
        let result = value.encode(&mut dest, 0);

        assert!(result.is_ok());

        // Length + Contents
        assert_eq!(dest.len(), 9);

        // Length Bytes
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x00);
        assert_eq!(dest[2], 0x00);
        assert_eq!(dest[3], 0x05);

        // Actual Content
        assert_eq!(dest[4], 0x0C);
        assert_eq!(dest[5], 0x80);
        assert_eq!(dest[6], 0xFF);
        assert_eq!(dest[7], 0x4E);
        assert_eq!(dest[8], 0x09);

        // Length in 32bits (4 bytes) + Contents (5 elements)
        assert_eq!(value.write_size(0), 9);
    }

    #[test]
    fn test_decode_bytebuf() {
        let data = [0x06, 0x64, 0x6f, 0x67];
        let mut bytebuf = ByteBuf::default();
        let result = bytebuf.decode(&mut Cursor::new(&data), 0);

        assert!(result.is_ok());
        assert_eq!(bytebuf.len(), 3);
        assert_eq!(bytebuf.inner[0], 0x64);
        assert_eq!(bytebuf.inner[1], 0x6f);
        assert_eq!(bytebuf.inner[2], 0x67);
    }
}
