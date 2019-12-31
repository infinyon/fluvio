use std::io::Read;
use serde::Deserialize;
use serde::Deserializer;
use serde::de::Visitor;
use bytes::Buf;
use log::trace;

use super::Error;
use super::ErrorKind;

pub struct KafkaDeserializer<B> {
    buf: B,
}

impl<B> KafkaDeserializer<B>
where
    B: Buf,
{
    pub fn from_buf(buf: B) -> Self {
        KafkaDeserializer { buf }
    }
}

#[allow(dead_code)]
pub fn from_buf<'a, T, B>(src: B) -> Result<T, Error>
where
    T: Deserialize<'a>,
    B: Buf,
{
    let mut k_der = KafkaDeserializer::from_buf(src);
    T::deserialize(&mut k_der)
}

impl<'de, 'a, B> Deserializer<'de> for &'a mut KafkaDeserializer<B>
where
    B: Buf,
{
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_any");
        Err(ErrorKind::DeserializeAnyNotSupported.into())
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        trace!("deserialize_bool");
        if self.buf.remaining() < 1 {
            return Err(Box::new(ErrorKind::NotEnoughBytes));
        }
        let value = self.buf.get_u8();

        match value {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            _ => Err(ErrorKind::InvalidBoolEncoding(value).into()),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_i8");
        if self.buf.remaining() < 1 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }
        let value = self.buf.get_i8();
        visitor.visit_i8(value)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        trace!("deserialize_u8");
        if self.buf.remaining() < 1 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }
        let value = self.buf.get_u8();

        visitor.visit_u8(value)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_u16");
        if self.buf.remaining() < 2 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_u16(self.buf.get_u16())
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_i16");

        if self.buf.remaining() < 2 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_i16(self.buf.get_i16())
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_u32");

        if self.buf.remaining() < 4 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_u32(self.buf.get_u32())
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_i32");

        if self.buf.remaining() < 4 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_i32(self.buf.get_i32())
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_u64");

        if self.buf.remaining() < 8 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_u64(self.buf.get_u64())
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_i64");

        if self.buf.remaining() < 8 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        visitor.visit_i64(self.buf.get_i64())
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("f32".to_owned()).into())
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("f64".to_owned()).into())
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("Unit".to_owned()).into())
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("char".to_owned()).into())
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("str slice".to_owned()).into())
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if self.buf.remaining() < 2 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        let len = self.buf.get_u16() as usize;
        if len <= 0 {
            return visitor.visit_string("".into());
        }

        let mut out_string = "".to_owned();
        let read_size = self
            .buf
            .by_ref()
            .take(len)
            .reader()
            .read_to_string(&mut out_string)?;

        if read_size != len {
            return Err(ErrorKind::Custom(format!(
                "not enough string, desired: {} but read: {}",
                len, read_size
            ))
            .into());
        }

        visitor.visit_string(out_string)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("bytes".to_owned()).into())
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("bytes buf".to_owned()).into())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserializing option: remaining: {}", self.buf.remaining());
        if self.buf.remaining() < 2 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }
        let mut buf = self.buf.by_ref().take(2);
        let len = buf.get_i16() as usize;

        trace!("check for length: remaining: {}", self.buf.remaining());
        if len == 0 {
            visitor.visit_none()
        } else {
            visitor.visit_some(&mut *self)
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("unit struct".to_owned()).into())
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("new type".to_owned()).into())
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize_sequence");

        if self.buf.remaining() < 4 {
            return Err(ErrorKind::NotEnoughBytes.into());
        }

        let len = self.buf.get_i32();
        self.deserialize_tuple(len as usize, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, B> {
            deserializer: &'a mut KafkaDeserializer<B>,
            len: usize,
        }

        impl<'de, 'a, B: Buf> serde::de::SeqAccess<'de> for Access<'a, B> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value =
                        (serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer))?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len: len,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("tuple struct".to_owned()).into())
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        trace!("deserialize map");
        Err(ErrorKind::NotSupportedFormat("map".to_owned()).into())
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("enum".to_owned()).into())
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("identifier".to_owned()).into())
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(ErrorKind::NotSupportedFormat("ignored".to_owned()).into())
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;
    use serde::Deserialize;
    use pretty_env_logger;

    use super::from_buf;
    use super::Error;

    fn init_logger() {
        let _ = pretty_env_logger::try_init();
    }

    #[derive(Deserialize, Debug, Default)]
    struct Dummy1 {
        value: u8,
        off: bool,
    }

    #[test]
    fn test_de_u8() -> Result<(), Error> {
        let data = [0x05, 0x01];

        let buf = &mut Cursor::new(&data);
        let dummy: Dummy1 = from_buf(buf)?;
        assert_eq!(dummy.value, 5);
        assert_eq!(dummy.off, true);
        Ok(())
    }

    #[derive(Deserialize, Debug, Default)]
    struct Dummy2 {
        value: i32,
    }

    #[test]
    fn test_serde_decode_i32() -> Result<(), Error> {
        init_logger();

        let data = [0x00, 0x00, 0x00, 0x10];

        let buf = &mut Cursor::new(&data);
        let dummy: Dummy2 = from_buf(buf)?;
        assert_eq!(dummy.value, 16);
        Ok(())
    }

    #[derive(Deserialize, Debug, Default)]
    struct Dummy3 {
        value: u32,
    }

    #[test]
    fn test_serde_decode_u32() -> Result<(), Error> {
        init_logger();

        let data = [0x00, 0x00, 0x00, 0x10];

        let buf = &mut Cursor::new(&data);
        let dummy: Dummy3 = from_buf(buf)?;
        assert_eq!(dummy.value, 16);
        Ok(())
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummyString {
        value: String,
    }

    #[test]
    fn test_serde_decode_string() -> Result<(), Error> {
        let data = [
            0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let buf = &mut Cursor::new(&data);
        let dummy: DummyString = from_buf(buf)?;
        assert_eq!(dummy.value, "consumer-1");
        Ok(())
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummySequence {
        value: Vec<u16>,
    }

    #[test]
    fn test_serde_decode_seq_u32() -> Result<(), Error> {
        let data = [0x00, 0x00, 0x00, 0x3, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03];

        let buf = &mut Cursor::new(&data);
        let dummy: DummySequence = from_buf(buf)?;
        assert_eq!(dummy.value.len(), 3);
        assert_eq!(dummy.value[0], 1);
        assert_eq!(dummy.value[1], 2);
        assert_eq!(dummy.value[2], 3);
        Ok(())
    }

    #[derive(Deserialize, Debug, Default)]
    struct DummyOptionString {
        value: Option<String>,
    }

    #[test]
    fn test_serde_decode_option_string() -> Result<(), Error> {
        init_logger();

        let data_none = [0x00, 0x00];

        let buf = &mut Cursor::new(&data_none);
        let dummy: DummyOptionString = from_buf(buf)?;
        assert!(dummy.value.is_none());

        let data_some = [
            0x00, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x2d, 0x31,
        ];

        let buf = &mut Cursor::new(&data_some);
        let dummy: DummyOptionString = from_buf(buf)?;
        assert!(dummy.value.is_some());
        let str_value = dummy.value.unwrap();
        // this will fail, we can't do look ahead with Buf trait
        // assert_eq!(str_value,"consumer-1");

        Ok(())
    }
}
