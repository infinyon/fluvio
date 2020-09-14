// decode values
use std::collections::BTreeMap;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Write;
use std::marker::PhantomData;

use bytes::buf::ext::BufMutExt;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use log::trace;

use crate::Version;

use super::varint::variant_encode;
use super::varint::variant_size;

// trait for encoding and decoding using Kafka Protocol
pub trait Encoder {
    /// size of this object in bytes
    fn write_size(&self, version: Version) -> usize;

    /// encoding contents for buffer
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut;

    fn as_bytes(&self, version: Version) -> Result<Bytes, Error> {
        trace!("encoding as bytes");
        let mut out = vec![];
        self.encode(&mut out, version)?;
        let mut buf = BytesMut::with_capacity(out.len());
        buf.put_slice(&out);
        Ok(buf.freeze())
    }
}

pub trait EncoderVarInt {
    fn var_write_size(&self) -> usize;

    /// encoding contents for buffer
    fn encode_varint<T>(&self, dest: &mut T) -> Result<(), Error>
    where
        T: BufMut;
}

impl<M> Encoder for Vec<M>
where
    M: Encoder,
{
    fn write_size(&self, version: Version) -> usize {
        self.iter()
            .fold(4, |sum, val| sum + val.write_size(version))
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for vec",
            ));
        }

        dest.put_u32(self.len() as u32);

        for ref v in self {
            v.encode(dest, version)?;
        }

        Ok(())
    }
}

impl<M> Encoder for Option<M>
where
    M: Encoder,
{
    fn write_size(&self, version: Version) -> usize {
        match *self {
            Some(ref value) => true.write_size(version) + value.write_size(version),
            None => false.write_size(version),
        }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        match *self {
            Some(ref value) => {
                true.encode(dest, version)?;
                value.encode(dest, version)
            }
            None => false.encode(dest, version),
        }
    }
}

impl<M> Encoder for PhantomData<M>
where
    M: Encoder,
{
    fn write_size(&self, _version: Version) -> usize {
        0
    }

    fn encode<T>(&self, _dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        Ok(())
    }
}

impl<K, V> Encoder for BTreeMap<K, V>
where
    K: Encoder,
    V: Encoder,
{
    fn write_size(&self, version: Version) -> usize {
        let mut len: usize = (0 as u16).write_size(version);

        for (key, value) in self.iter() {
            len += key.write_size(version);
            len += value.write_size(version);
        }

        len
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        let len = self.len() as u16;
        len.encode(dest, version)?;

        for (key, value) in self.iter() {
            key.encode(dest, version)?;
            value.encode(dest, version)?;
        }

        Ok(())
    }
}

impl Encoder for bool {
    fn write_size(&self, _version: Version) -> usize {
        1
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for bool",
            ));
        }
        if *self {
            dest.put_i8(1);
        } else {
            dest.put_i8(0);
        }
        Ok(())
    }
}

impl Encoder for i8 {
    fn write_size(&self, _version: Version) -> usize {
        1
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for i8",
            ));
        }
        dest.put_i8(*self);
        Ok(())
    }
}

impl Encoder for u8 {
    fn write_size(&self, _version: Version) -> usize {
        1
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for i8",
            ));
        }
        dest.put_u8(*self);
        Ok(())
    }
}

impl Encoder for i16 {
    fn write_size(&self, _version: Version) -> usize {
        2
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 2 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for i16",
            ));
        }
        dest.put_i16(*self);
        trace!("encoding i16: {:#x}", *self);
        Ok(())
    }
}

impl Encoder for u16 {
    fn write_size(&self, _version: Version) -> usize {
        2
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 2 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for u16",
            ));
        }
        dest.put_u16(*self);
        trace!("encoding u16: {:#x}", *self);
        Ok(())
    }
}

impl Encoder for i32 {
    fn write_size(&self, _version: Version) -> usize {
        4
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for i32",
            ));
        }
        dest.put_i32(*self);
        trace!("encoding i32: {:#x}", *self);
        Ok(())
    }
}

impl Encoder for u32 {
    fn write_size(&self, _version: Version) -> usize {
        4
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for u32",
            ));
        }
        dest.put_u32(*self);
        Ok(())
    }
}

impl Encoder for i64 {
    fn write_size(&self, _version: Version) -> usize {
        8
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 8 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for i164",
            ));
        }
        dest.put_i64(*self);
        Ok(())
    }
}

impl EncoderVarInt for i64 {
    fn var_write_size(&self) -> usize {
        variant_size(*self)
    }

    fn encode_varint<T>(&self, dest: &mut T) -> Result<(), Error>
    where
        T: BufMut,
    {
        variant_encode(dest, *self)?;
        Ok(())
    }
}

impl Encoder for String {
    fn write_size(&self, _version: Version) -> usize {
        2 + self.len()
    }

    fn encode<T>(&self, dest: &mut T, _version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 2 + self.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for string",
            ));
        }

        dest.put_u16(self.len() as u16);

        let mut writer = dest.writer();
        let bytes_written = writer.write(self.as_bytes())?;

        if bytes_written != self.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "out of {} bytes, {} not written",
                    self.len(),
                    self.len() - bytes_written
                ),
            ));
        }

        Ok(())
    }
}

impl EncoderVarInt for Option<Vec<u8>> {
    fn var_write_size(&self) -> usize {
        if self.is_none() {
            let len: i64 = -1;
            return variant_size(len);
        }

        let b_values = self.as_ref().unwrap();

        let len: i64 = b_values.len() as i64;
        let bytes = variant_size(len);

        bytes + b_values.len()
    }

    fn encode_varint<T>(&self, dest: &mut T) -> Result<(), Error>
    where
        T: BufMut,
    {
        if self.is_none() {
            let len: i64 = -1;
            variant_encode(dest, len)?;
            return Ok(());
        }

        let b_values = self.as_ref().unwrap();

        let len: i64 = b_values.len() as i64;
        len.encode_varint(dest)?;

        if dest.remaining_mut() < b_values.len() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("not enough capacity for byte array: {}", b_values.len()),
            ));
        }

        dest.put_slice(b_values);

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use bytes::BufMut;
    use std::io::Error as IoError;

    use crate::Encoder;
    use crate::EncoderVarInt;
    use crate::Version;

    #[test]
    fn test_encode_i8() {
        let mut dest = vec![];
        let value: i8 = 5;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x05);
        assert_eq!(value.write_size(0), 1);
    }

    #[test]
    fn test_encode_u8() {
        let mut dest = vec![];
        let value: u8 = 8;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x08);
        assert_eq!(value.write_size(0), 1);
    }

    #[test]
    fn test_encode_i16() {
        let mut dest = vec![];
        let value: i16 = 5;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 2);
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x05);
        assert_eq!(value.write_size(0), 2);
    }

    #[test]
    fn test_encode_u16() {
        let mut dest = vec![];
        let value: u16 = 16;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 2);
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x10);
        assert_eq!(value.write_size(0), 2);
    }

    #[test]
    fn test_encode_option_u16_none() {
        let mut dest = vec![];
        let value: Option<u16> = None;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x00);
        assert_eq!(value.write_size(0), 1);
    }

    #[test]
    fn test_encode_option_u16_with_val() {
        let mut dest = vec![];
        let value: Option<u16> = Some(16);
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 3);
        assert_eq!(dest[0], 0x01);
        assert_eq!(dest[1], 0x00);
        assert_eq!(dest[2], 0x10);
        assert_eq!(value.write_size(0), 3);
    }

    #[test]
    fn test_encode_i32() {
        let mut dest = vec![];
        let value: i32 = 5;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 4);
        assert_eq!(dest[3], 0x05);
        assert_eq!(value.write_size(0), 4);
    }

    #[test]
    fn test_encode_i64() {
        let mut dest = vec![];
        let value: i64 = 5;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 8);
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[7], 0x05);
        assert_eq!(value.write_size(0), 8);
    }

    #[test]
    fn test_encode_string_option_none() {
        let mut dest = vec![];
        let value: Option<String> = None;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x00);
        assert_eq!(value.write_size(0), 1);
    }

    #[test]
    fn test_encode_string_option_some() {
        let mut dest = vec![];
        let value: Option<String> = Some(String::from("wo"));
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 5);
        assert_eq!(dest[0], 0x01);
        assert_eq!(dest[1], 0x00);
        assert_eq!(dest[2], 0x02);
        assert_eq!(dest[3], 0x77);
        assert_eq!(dest[4], 0x6f);
        assert_eq!(value.write_size(0), 5);
    }

    #[test]
    fn test_encode_string() {
        let mut dest = vec![];
        let value = String::from("wo");
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 4);
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x02);
        assert_eq!(dest[2], 0x77);
        assert_eq!(dest[3], 0x6f);
        assert_eq!(value.write_size(0), 4);
    }

    #[test]
    fn test_encode_bool() {
        let mut dest = vec![];
        let value = true;
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x01);
        assert_eq!(value.write_size(0), 1);
    }

    #[test]
    fn test_encode_string_vectors() {
        let mut dest = vec![];
        let value: Vec<String> = vec![String::from("test")];
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 10);
        assert_eq!(dest[3], 0x01);
        assert_eq!(dest[9], 0x74);
        assert_eq!(value.write_size(0), 10); // vec len 4: string len: 2, string 4
    }

    #[test]
    fn test_encode_u8_vectors() {
        let mut dest = vec![];
        let value: Vec<u8> = vec![0x10, 0x11];
        let result = value.encode(&mut dest, 0);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 6);
        assert_eq!(dest[3], 0x02);
        assert_eq!(dest[5], 0x11);
        assert_eq!(value.write_size(0), 6);
    }

    #[test]
    fn test_varint_encode_array_opton_vec8_none() {
        let mut dest = vec![];
        let value: Option<Vec<u8>> = None;
        let result = value.encode_varint(&mut dest);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x01);
    }

    #[test]
    fn test_varint_encode_array_opton_vec8_simple_array() {
        let mut dest = vec![];
        let value: Option<Vec<u8>> = Some(vec![0x64, 0x6f, 0x67]);
        let result = value.encode_varint(&mut dest);
        assert!(result.is_ok());
        assert_eq!(dest.len(), 4);
    }

    #[derive(Default)]
    struct TestRecord {
        value: i8,
        value2: i8,
    }

    impl Encoder for TestRecord {
        fn write_size(&self, version: Version) -> usize {
            self.value.write_size(version) + {
                if version > 1 {
                    self.value2.write_size(version)
                } else {
                    0
                }
            }
        }

        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            self.value.encode(dest, version)?;
            if version > 1 {
                self.value2.encode(dest, version)?;
            }
            Ok(())
        }
    }

    #[test]
    fn test_encoding_struct() {
        // v1
        let mut dest = vec![];
        let mut record = TestRecord::default();
        record.value = 20;
        record.value2 = 10;
        record.encode(&mut dest, 0).expect("encode");
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 20);
        assert_eq!(record.write_size(0), 1);

        let mut dest2 = vec![];
        record.encode(&mut dest2, 2).expect("encodv2 encodee");
        assert_eq!(dest2.len(), 2);
        assert_eq!(dest2[1], 10);

        // v2
        /*
        let data2 = [0x06,0x09];
        let record2 = TestRecord::decode_from(&mut Cursor::new(&data2),2).expect("decode");
        assert_eq!(record2.value, 6);
        assert_eq!(record2.value2, 9);
        */
    }
}
