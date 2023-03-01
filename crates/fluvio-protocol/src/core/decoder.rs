use std::cmp::Ord;
use std::collections::BTreeMap;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::marker::PhantomData;

use bytes::Buf;
use bytes::BufMut;
use tracing::trace;

use super::varint::varint_decode;
use crate::Version;

// trait for encoding and decoding using Kafka Protocol
pub trait Decoder: Sized + Default {
    /// decode Fluvio compliant protocol values from buf
    fn decode_from<T>(src: &mut T, version: Version) -> Result<Self, Error>
    where
        T: Buf,
    {
        let mut decoder = Self::default();
        decoder.decode(src, version)?;
        Ok(decoder)
    }

    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf;
}

pub trait DecoderVarInt {
    fn decode_varint<T>(&mut self, src: &mut T) -> Result<(), Error>
    where
        T: Buf;
}

impl<M> Decoder for Vec<M>
where
    M: Decoder,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: i32 = 0;
        len.decode(src, version)?;

        trace!("decoding Vec len:{}", len);

        if len < 1 {
            trace!("negative length, skipping");
            return Ok(());
        }

        decode_vec(len, self, src, version)?;

        Ok(())
    }
}

fn decode_vec<T, M>(len: i32, item: &mut Vec<M>, src: &mut T, version: Version) -> Result<(), Error>
where
    T: Buf,
    M: Default + Decoder,
{
    for _ in 0..len {
        let mut value = <M>::default();
        value.decode(src, version)?;
        item.push(value);
    }

    Ok(())
}

impl<M> Decoder for Option<M>
where
    M: Decoder,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut some = false;
        some.decode(src, version)?;
        if some {
            let mut value = <M>::default();
            value.decode(src, version)?;
            *self = Some(value)
        } else {
            *self = None
        }
        Ok(())
    }
}

impl<M> Decoder for PhantomData<M> {
    fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        Ok(())
    }
}

impl<K, V> Decoder for BTreeMap<K, V>
where
    K: Decoder + Ord,
    V: Decoder,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: u16 = 0;
        len.decode(src, version)?;

        let mut map: BTreeMap<K, V> = BTreeMap::new();
        for _i in 0..len {
            let mut key = K::default();
            key.decode(src, version)?;
            let mut value = V::default();
            value.decode(src, version)?;
            map.insert(key, value);
        }

        *self = map;
        Ok(())
    }
}

impl Decoder for bool {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough buf for bool",
            ));
        }
        let value = src.get_u8();

        match value {
            0 => *self = false,
            1 => *self = true,
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, "not valid bool value"));
            }
        };

        Ok(())
    }
}

impl Decoder for i8 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough buf for i8",
            ));
        }
        let value = src.get_i8();
        *self = value;
        Ok(())
    }
}

impl Decoder for u8 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 1 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough buf for u8",
            ));
        }
        let value = src.get_u8();
        *self = value;
        Ok(())
    }
}

impl Decoder for i16 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 2 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read i16"));
        }
        let value = src.get_i16();
        *self = value;
        Ok(())
    }
}

impl Decoder for u16 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 2 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read u16"));
        }
        let value = src.get_u16();
        *self = value;
        Ok(())
    }
}

impl Decoder for i32 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 4 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read i32"));
        }
        let value = src.get_i32();
        trace!("i32: {:#x} => {}", &value, &value);
        *self = value;
        Ok(())
    }
}

impl Decoder for u32 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 4 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read u32"));
        }
        let value = src.get_u32();
        trace!("u32: {:#x} => {}", &value, &value);
        *self = value;
        Ok(())
    }
}

impl Decoder for u64 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 8 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read u64"));
        }
        let value = src.get_u64();
        trace!("u64: {:#x} => {}", &value, &value);
        *self = value;
        Ok(())
    }
}

impl Decoder for i64 {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 8 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "can't read i64"));
        }
        let value = src.get_i64();
        trace!("i64: {:#x} => {}", &value, &value);
        *self = value;
        Ok(())
    }
}

impl DecoderVarInt for i64 {
    fn decode_varint<T>(&mut self, src: &mut T) -> Result<(), Error>
    where
        T: Buf,
    {
        let (value, _) = varint_decode(src)?;
        *self = value;
        Ok(())
    }
}

fn decode_string<T>(len: i16, src: &mut T) -> Result<String, Error>
where
    T: Buf,
{
    let mut value = String::default();
    let read_size = src.take(len as usize).reader().read_to_string(&mut value)?;

    if read_size != len as usize {
        return Err(Error::new(ErrorKind::UnexpectedEof, "not enough string"));
    }
    Ok(value)
}

impl Decoder for String {
    fn decode<T>(&mut self, src: &mut T, _version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 2 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "can't read string length",
            ));
        }
        let len = src.get_i16();
        if len <= 0 {
            return Ok(());
        }

        let value = decode_string(len, src)?;
        *self = value;
        Ok(())
    }
}

impl DecoderVarInt for Vec<u8> {
    fn decode_varint<T>(&mut self, src: &mut T) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        if len < 1 {
            return Ok(());
        }

        let mut buf = src.take(len as usize);
        self.put(&mut buf);
        if self.len() != len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "varint: Vec<u8>>, expecting {} but received: {}",
                    len,
                    self.len()
                ),
            ));
        }

        Ok(())
    }
}

fn decode_option_vec_u<T>(array: &mut Option<Vec<u8>>, src: &mut T, len: isize) -> Result<(), Error>
where
    T: Buf,
{
    if len < 0 {
        *array = None;
        return Ok(());
    }

    if len == 0 {
        *array = Some(Vec::new());
        return Ok(());
    }

    let mut buf = src.take(len as usize);
    let mut value: Vec<u8> = Vec::new();
    value.put(&mut buf);
    if value.len() != len as usize {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!(
                "Option<Vec<u8>>>, expecting {} but received: {}",
                len,
                value.len()
            ),
        ));
    }

    *array = Some(value);

    Ok(())
}

impl DecoderVarInt for Option<Vec<u8>> {
    fn decode_varint<T>(&mut self, src: &mut T) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        decode_option_vec_u(self, src, len as isize)
    }
}

#[cfg(test)]
mod test {

    use crate::Decoder;
    use crate::DecoderVarInt;
    use crate::Version;
    use bytes::Buf;
    use std::io::Cursor;
    use std::io::Error;

    #[test]
    fn test_decode_i18_not_enough() {
        let data = []; // no values
        let mut value: i8 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_i8() {
        let data = [0x12];

        let mut value: i8 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 18);
    }

    #[test]
    fn test_decode_u18_not_enough() {
        let data = []; // no values
        let mut value: u8 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_u8() {
        let data = [0x12];

        let mut value: u8 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 18);
    }

    #[test]
    fn test_decode_i16_not_enough() {
        let data = [0x11]; // only one value

        let mut value: i16 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_i16() {
        let data = [0x00, 0x05];

        let mut value: i16 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 5);
    }

    #[test]
    fn test_decode_u16_not_enough() {
        let data = [0x11]; // only one value

        let mut value: u16 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_u16() {
        let data = [0x00, 0x05];

        let mut value: u16 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 5);
    }

    #[test]
    fn test_decode_option_u16_none() {
        let data = [0x00];

        let mut value: Option<u16> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, None);
    }

    #[test]
    fn test_decode_option_u16_val() {
        let data = [0x01, 0x00, 0x10];

        let mut value: Option<u16> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, Some(16));
    }

    #[test]
    fn test_decode_u32_not_enough() {
        let data = [0x11];

        let mut value: u32 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_u32() {
        let data = [0x00, 0x00, 0x00, 0x05];

        let mut value: u32 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 5);
    }

    #[test]
    fn test_decode_option_u32_none() {
        let data = [0x00];

        let mut value: Option<u32> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, None);
    }

    #[test]
    fn test_decode_option_u32_val() {
        let data = [0x01, 0x00, 0x00, 0x01, 0x10];

        let mut value: Option<u32> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, Some(272));
    }

    #[test]
    fn test_decode_u64_not_enough() {
        let data = [0x11];

        let mut value: u64 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_u64() {
        let data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];

        let mut value: u64 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 5);
    }

    #[test]
    fn test_decode_option_u64_none() {
        let data = [0x00];

        let mut value: Option<u64> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, None);
    }

    #[test]
    fn test_decode_option_u64_val() {
        let data = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];

        let mut value: Option<u64> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, Some(5));
    }

    #[test]
    fn test_decode_i32_not_enough() {
        let data = [0x11, 0x11, 0x00]; // still need one more

        let mut value: i32 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_i32() {
        let data = [0x00, 0x00, 0x00, 0x10];

        let mut value: i32 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 16);
    }

    #[test]
    fn test_decode_i32_2() {
        let data = [0x00, 0x00, 0x00, 0x01];

        let mut value: i32 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 1);
    }

    #[test]
    fn test_decode_i64_not_enough() {
        let data = [0x11, 0x11, 0x00]; // still need one more

        let mut value: i64 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_i64() {
        let data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20];

        let mut value: i64 = 0;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, 32);
    }

    #[test]
    fn test_decode_invalid_string_not_len() {
        let data = [0x11]; // doesn't have right bytes

        let mut value = String::from("");
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_invalid_string() {
        let data = [0x00, 0x0a, 0x63]; // len and string doesn't match

        let mut value = String::from("");
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_null_option_string() {
        let data = [0x00]; // len and string doesn't match

        let mut value: Option<String> = Some(String::from("test"));
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert!(value.is_none());
    }

    #[test]
    fn test_decode_some_option_string() {
        let data = [0x01, 0x00, 0x02, 0x77, 0x6f]; // len and string doesn't match

        let mut value: Option<String> = None;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "wo");
    }

    #[test]
    fn test_decode_string_existing_value() {
        let src = [0x0, 0x7, 0x30, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x30];
        let mut decode_target = "123".to_string();
        let result = decode_target.decode(&mut Cursor::new(&src), 0);
        assert!(result.is_ok());
        assert_eq!(decode_target, "0.0.0.0".to_string());
    }

    #[test]
    fn test_decode_string() {
        let data = [
            0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let mut value = String::from("");
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, "consumer-1");
    }

    #[test]
    fn test_decode_bool_not_enough() {
        let data = []; // no values

        let mut value: bool = false;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_bool() {
        let data = [0x1];

        let mut value: bool = false;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert!(value);
    }

    #[test]
    fn test_decode_bool_invalid_value() {
        let data = [0x23]; // not bool

        let mut value: bool = false;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_valid_string_vectors() {
        // array of strings with "test"
        let data = [0, 0, 0, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74];

        let mut values: Vec<String> = Vec::new();
        let result = values.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(values.len(), 1);
        let first_str = &values[0];
        assert_eq!(first_str, "test");
    }

    #[test]
    fn test_decode_varint_trait() {
        let data = [0x7e];

        let mut value: i64 = 0;
        let result = value.decode_varint(&mut Cursor::new(&data));
        assert!(result.is_ok());
        assert_eq!(value, 63);
    }

    #[test]
    fn test_decode_varint_vec8() {
        let data = [0x06, 0x64, 0x6f, 0x67];

        let mut value: Vec<u8> = Vec::new();
        let result = value.decode_varint(&mut Cursor::new(&data));
        assert!(result.is_ok());
        assert_eq!(value.len(), 3);
        assert_eq!(value[0], 0x64);
    }

    #[test]
    fn test_vec8_encode_and_decode() {
        use crate::Encoder;
        let in_vec: Vec<u8> = vec![1, 2, 3];
        let mut out: Vec<u8> = vec![];
        let ret = in_vec.encode(&mut out, 0);
        assert!(ret.is_ok());
    }

    #[test]
    fn test_decode_varint_vec8_fail() {
        let data = [0x06, 0x64, 0x6f];

        let mut value: Vec<u8> = Vec::new();
        let result = value.decode_varint(&mut Cursor::new(&data));
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_decode_array_opton_vec8_simple_array() {
        let data = [0x06, 0x64, 0x6f, 0x67, 0x00]; // should only read first 3

        let mut value: Option<Vec<u8>> = Some(Vec::new());
        let result = value.decode_varint(&mut Cursor::new(&data));
        assert!(result.is_ok());
        assert!(value.is_some());
        let array = value.unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array[0], 0x64);
    }

    #[derive(Default)]
    struct TestRecord {
        value: i8,
        value2: i8,
    }

    impl Decoder for TestRecord {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            self.value.decode(src, 0)?;
            if version > 1 {
                self.value2.decode(src, 0)?;
            }
            Ok(())
        }
    }

    #[test]
    fn test_decoding_struct() {
        let data = [0x06];

        // v1
        let result = TestRecord::decode_from(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        let record = result.unwrap();
        assert_eq!(record.value, 6);
        assert_eq!(record.value2, 0);

        // v2
        let data2 = [0x06, 0x09];
        let record2 = TestRecord::decode_from(&mut Cursor::new(&data2), 2).expect("decode");
        assert_eq!(record2.value, 6);
        assert_eq!(record2.value2, 9);
    }
}
