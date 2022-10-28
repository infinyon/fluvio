use std::io::Error;
use std::io::ErrorKind;

use bytes::buf::UninitSlice;
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
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
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
        core::isize::MAX as usize - self.inner.len()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let len = self.len();
        let remaining = self.inner.capacity() - len;

        assert!(
            cnt <= remaining,
            "cannot advance past `remaining_mut`: {:?} <= {:?}",
            cnt,
            remaining
        );

        self.inner.set_len(len + cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        if self.inner.capacity() == self.len() {
            self.inner.reserve(64); // Grow the vec
        }

        let cap = self.inner.capacity();
        let len = self.inner.len();

        let ptr = self.inner.as_mut_ptr();
        unsafe { &mut UninitSlice::from_raw_parts_mut(ptr, cap)[len..] }
    }

    fn put<T: Buf>(&mut self, mut src: T)
    where
        Self: Sized,
    {
        self.inner.reserve(src.remaining());

        while src.has_remaining() {
            let l;

            {
                let s = src.chunk();
                l = s.len();
                self.inner.extend_from_slice(s);
            }

            src.advance(l);
        }
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.inner.extend_from_slice(src);
    }

    fn put_bytes(&mut self, val: u8, cnt: usize) {
        let new_len = self.len().checked_add(cnt).unwrap();
        self.inner.resize(new_len, val);
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

    // #[test]
    // fn test_enc_dec_is_simetric() {
    //     // let src = [0x06, 0x64, 0x6f, 0x67, 0x11, 0x12, 0x09, 0x28];
    //     // let mut dest = Vec::default();
    //     // let mut bytebuf = ByteBuf::default();
    //     // let decode_res = bytebuf.decode(&mut Cursor::new(&src), 0);
    //     // let encode_res = bytebuf.encode(&mut dest, 0);

    //     // assert!(decode_res.is_ok());
    //     // assert!(encode_res.is_ok());
    //     // assert_eq!(Vec::from(src), dest);
    //     assert!(true);
    // }

    #[test]
    fn test_encodes_as_vec_u8() {
        let data: Vec<u8> = vec![0x06, 0x64, 0x6f, 0x67, 0x11, 0x12, 0x09, 0x28];

        let mut vecu8_dest = Vec::default();
        let encode_vecu8_res = data.encode(&mut vecu8_dest, 0);

        let mut bytebuf_dest = ByteBuf::default();
        let encode_bytebuf_res = data.encode(&mut bytebuf_dest, 0);

        assert!(encode_vecu8_res.is_ok());
        assert_eq!(vecu8_dest.len(), 8 + 4);
        assert_eq!(vecu8_dest[3], 0x08);
        assert_eq!(vecu8_dest[4], 0x06);
        assert_eq!(vecu8_dest[5], 0x64);
        assert_eq!(vecu8_dest[6], 0x6f);
        assert_eq!(vecu8_dest[7], 0x67);
        assert_eq!(vecu8_dest[8], 0x11);
        assert_eq!(data.write_size(0), vecu8_dest.len());

        assert!(encode_bytebuf_res.is_ok());
        assert_eq!(bytebuf_dest.len(), 8 + 4);
        assert_eq!(bytebuf_dest.inner[3], 0x08);
        assert_eq!(bytebuf_dest.inner[4], 0x06);
        assert_eq!(bytebuf_dest.inner[5], 0x64);
        assert_eq!(bytebuf_dest.inner[6], 0x6f);
        assert_eq!(bytebuf_dest.inner[7], 0x67);
        assert_eq!(bytebuf_dest.inner[8], 0x11);
        assert_eq!(data.write_size(0), bytebuf_dest.len());
    }

    // #[test]
    // fn test_decodes_as_vec_u8() {
    //     let data = [0x06, 0x64, 0x6f, 0x67, 0xaa];

    //     let mut vecu8_val: Vec<u8> = Vec::new();
    //     let vecu8_res = vecu8_val.decode(&mut Cursor::new(&data), 0).unwrap();

    //     // assert!(vecu8_res.is_ok());
    //     assert_eq!(vecu8_val.len(), 7 + 1);
    //     assert_eq!(vecu8_val[0], 0x64);
    //     // let mut dest = Vec::default();
    //     // let mut bytebuf = ByteBuf::default();
    //     // let decode_res = bytebuf.decode(&mut Cursor::new(&src), 0);
    //     // let encode_res = bytebuf.encode(&mut dest, 0);

    //     // assert!(decode_res.is_ok());
    //     // assert!(encode_res.is_ok());
    //     // assert_eq!(Vec::from(src), dest);
    //     assert!(true);
    // }
}
