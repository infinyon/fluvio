use std::io::Error;
use std::io::ErrorKind;

use bytes::{Buf, Bytes, BufMut};

use crate::{Encoder, Decoder, Version};
use crate::core::decoder::DecoderVarInt;

/// Represnts a SmartModule WASM File bytes.
///
/// Provides a `Encoder` and `Decoder` implementation optimized for WASM files
/// used in SmartModules.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ByteBuf {
    #[cfg_attr(feature = "use_serde", serde(with = "base64"))]
    bytes: Vec<u8>,
}

impl ByteBuf {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl From<Vec<u8>> for ByteBuf {
    fn from(bytes: Vec<u8>) -> Self {
        ByteBuf { bytes }
    }
}

impl Decoder for ByteBuf {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        if len < 1 {
            return Ok(());
        }

        self.bytes.extend_from_slice(&src.chunk());

        Ok(())
    }
}

impl Encoder for ByteBuf {
    fn write_size(&self, _version: Version) -> usize {
        self.bytes.len() + 4
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

        dest.put_u32(self.bytes.len() as u32);
        dest.put_slice(self.bytes.as_slice());

        Ok(())
    }

    fn as_bytes(&self, _version: Version) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(self.bytes.as_slice()))
    }
}

#[cfg(feature = "use_serde")]
mod base64 {
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    #[allow(clippy::ptr_arg)]
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{Decoder, Encoder};
    use super::ByteBuf;

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
        let mut value = ByteBuf::default();
        let data: &[u8] = &[0x06, 0x64, 0x6f, 0x67];
        let result = value.decode(&mut Cursor::new(&data), 0);

        assert!(result.is_ok());
        assert_eq!(value.bytes.len(), 3);
        assert_eq!(value.bytes[0], 0x64);
        assert_eq!(value.bytes[1], 0x6f);
        assert_eq!(value.bytes[2], 0x67);
    }
}
