use std::io::Error as IoError;
use std::io::ErrorKind;

use bytes::{Bytes, BufMut};

use crate::{Encoder, Version};

pub struct WasmBytes(Vec<u8>);

impl From<Vec<u8>> for WasmBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl Encoder for WasmBytes {
    #[inline]
    fn write_size(&self, _version: Version) -> usize {
        4
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        let remaining = dest.remaining_mut();
        let expected = self.write_size(version);

        if remaining < expected {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Not enough capacity for WasmBytes. Expected: {}, Remaining: {}",
                    expected, remaining
                ),
            ));
        }

        dest.put_u32(self.0.len() as u32);
        dest.put_slice(self.0.as_slice());

        Ok(())
    }

    fn as_bytes(&self, _version: Version) -> Result<Bytes, IoError> {
        Ok(Bytes::copy_from_slice(self.0.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use crate::Encoder;
    use super::WasmBytes;

    #[test]
    fn test_encode_wasmbytes() {
        let mut dest = Vec::default();
        let value: WasmBytes = WasmBytes::from(vec![12, 128, 255, 78, 9]);
        let result = value.encode(&mut dest, 0);

        assert!(result.is_ok());
        assert_eq!(dest.len(), 5);
        assert_eq!(dest[0], 0x0C);
        assert_eq!(dest[1], 0x80);
        assert_eq!(dest[2], 0xFF);
        assert_eq!(dest[3], 0x4E);
        assert_eq!(dest[4], 0x09);
        assert_eq!(value.write_size(0), 1);
    }
}