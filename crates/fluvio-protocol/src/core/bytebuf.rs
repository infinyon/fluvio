use std::io::Error;
use std::io::ErrorKind;

use bytes::{Buf, Bytes, BufMut};

use crate::{Encoder, Decoder, Version};

/// Represnts a SmartModule WASM File bytes.
///
/// Provides a `Encoder` implementation optimized for WASM files used in
/// SmartModules.
#[derive(Clone, Debug, Default)]
pub struct ByteBuf(Vec<u8>);

impl From<Vec<u8>> for ByteBuf {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
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

        src.copy_to_slice(&mut self.0);

        Ok(())
    }
}

impl Encoder for ByteBuf {
    fn write_size(&self, _version: Version) -> usize {
        self.0.len() + 4
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

        dest.put_u32(self.0.len() as u32);
        dest.put_slice(self.0.as_slice());

        Ok(())
    }

    fn as_bytes(&self, _version: Version) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(self.0.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use crate::Encoder;
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
}
