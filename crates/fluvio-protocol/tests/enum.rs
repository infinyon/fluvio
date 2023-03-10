use std::convert::TryInto;
use std::io::Cursor;
use std::io::Error;
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_protocol::bytes::{Buf, BufMut};
use fluvio_protocol::{Decoder, Encoder, Version};

// manual encode
#[derive(Default)]
pub enum Mix {
    #[default]
    A = 2,
    C = 3,
}

impl Encoder for Mix {
    fn write_size(&self, _version: Version) -> usize {
        match self {
            Mix::A => 2,
            Mix::C => 2,
        }
    }

    fn encode<T>(&self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        match self {
            Mix::A => {
                let val = 2_u8;
                val.encode(src, version)?;
            }
            Mix::C => {
                let val = 3_u8;
                val.encode(src, version)?;
            }
        }
        Ok(())
    }
}

impl Decoder for Mix {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            2 => {
                *self = Mix::A;
            }
            3 => {
                *self = Mix::C;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for Mix: {value}"),
                ))
            }
        }

        Ok(())
    }
}

#[derive(Encoder, Debug)]
pub enum UnitAndDataEnum {
    #[fluvio(tag = 0)]
    UnitVariant,
    #[fluvio(tag = 1)]
    DataVariant(i16),
}

#[derive(Encoder, Debug)]
pub enum VariantEnum {
    #[fluvio(tag = 0)]
    A(u16),
    #[fluvio(tag = 1)]
    C(String),
}

#[test]
fn test_var_encode() {
    let v1 = VariantEnum::C("hello".to_string());
    let mut src = vec![];
    let result = v1.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 8);
    assert_eq!(v1.write_size(0), 8);
}

#[derive(Encoder, Decoder, Debug)]
pub enum NamedEnum {
    #[fluvio(tag = 0)]
    Apple { seeds: u16, color: String },
    #[fluvio(tag = 1)]
    Banana { peel: bool },
}

impl Default for NamedEnum {
    fn default() -> Self {
        Self::Banana { peel: true }
    }
}

#[test]
fn test_named_encode() {
    let apple = NamedEnum::Apple {
        seeds: 13,
        color: "Red".into(),
    };
    let mut dest = Vec::new();
    apple.encode(&mut dest, 0).unwrap();
    let expected = vec![0x00, 0x00, 0x0d, 0x00, 0x03, 0x52, 0x65, 0x64];
    assert_eq!(expected, dest);
}

#[test]
fn test_named_decode() {
    let data = vec![0x00, 0x00, 0x0d, 0x00, 0x03, 0x52, 0x65, 0x64];
    let mut value = NamedEnum::default();
    value.decode(&mut std::io::Cursor::new(data), 0).unwrap();
    match value {
        NamedEnum::Apple { seeds, color } => {
            assert_eq!(seeds, 13);
            assert_eq!(color, "Red");
        }
        _ => panic!("failed to decode"),
    }
}

#[derive(Encoder, Decoder, Debug)]
enum NamedCustomTag {
    #[fluvio(tag = 22)]
    One { a: String, b: i32 },
    #[fluvio(tag = 44)]
    Two { c: i64 },
}

impl Default for NamedCustomTag {
    fn default() -> Self {
        Self::Two { c: 999 }
    }
}

#[test]
fn test_named_custom_tag_encode() {
    let value = NamedCustomTag::One {
        a: "Hello".to_string(),
        b: 234,
    };
    let mut dest = Vec::new();
    value.encode(&mut dest, 0).unwrap();

    let expected = vec![
        0x16, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0xea,
    ];
    assert_eq!(dest, expected);
}

#[test]
fn test_named_custom_tag_decode() {
    let data = vec![
        0x16, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0xea,
    ];
    let mut value = NamedCustomTag::default();
    value.decode(&mut std::io::Cursor::new(data), 0).unwrap();

    match value {
        NamedCustomTag::One { a, b } => {
            assert_eq!(a, "Hello");
            assert_eq!(b, 234);
        }
        _ => panic!("Failed decode"),
    }
}

#[derive(Encoder, Decoder, Debug)]
pub enum MultiUnnamedEnum {
    #[fluvio(tag = 0)]
    Apple(u16, String),
    #[fluvio(tag = 1)]
    Banana(bool),
}

impl Default for MultiUnnamedEnum {
    fn default() -> Self {
        Self::Banana(true)
    }
}

#[test]
fn test_multi_unnamed_encode() {
    let apple = MultiUnnamedEnum::Apple(13, "Red".into());
    let mut dest = Vec::new();
    apple.encode(&mut dest, 0).unwrap();

    let expected = vec![0x00, 0x00, 0x0d, 0x00, 0x03, 0x52, 0x65, 0x64];
    assert_eq!(expected, dest);
}

#[test]
fn test_multi_unnamed_decode() {
    let data = vec![0x00, 0x00, 0x0d, 0x00, 0x03, 0x52, 0x65, 0x64];
    let mut value = MultiUnnamedEnum::default();
    value.decode(&mut std::io::Cursor::new(data), 0).unwrap();

    match value {
        MultiUnnamedEnum::Apple(num, string) => {
            assert_eq!(num, 13);
            assert_eq!(string, "Red");
        }
        _ => panic!("Failed to decode"),
    }
}

#[derive(Debug, Encoder, Decoder)]
enum MultiUnnamedCustomTag {
    #[fluvio(tag = 7)]
    Rgb(u8, u8, u8),
    #[fluvio(tag = 70)]
    Hsv(u8, u8, u8),
    #[fluvio(tag = 77)]
    ColorName(String),
}

impl Default for MultiUnnamedCustomTag {
    fn default() -> Self {
        Self::Rgb(0, 0, 0)
    }
}

#[test]
fn test_multi_unnamed_custom_tag_encode() {
    let mut dest = vec![];
    let value = MultiUnnamedCustomTag::Hsv(22, 33, 44);
    value.encode(&mut dest, 0).unwrap();

    let expected = vec![0x46, 0x16, 0x21, 0x2c];
    assert_eq!(dest, expected);
}

#[test]
fn test_multi_unnamed_custom_tag_decode() {
    let data = vec![0x46, 0x16, 0x21, 0x2c];
    let mut value = MultiUnnamedCustomTag::default();
    value.decode(&mut std::io::Cursor::new(data), 0).unwrap();

    match value {
        MultiUnnamedCustomTag::Hsv(22, 33, 44) => (),
        _ => panic!("failed decode"),
    }
}

#[derive(Default, Encoder, Eq, PartialEq, Decoder, Debug)]
#[repr(u8)]
pub enum EnumNoExprTest {
    #[default]
    #[fluvio(tag = 0)]
    A,
    #[fluvio(tag = 1)]
    B,
}

#[test]
fn test_enum_encode() {
    let v1 = EnumNoExprTest::B;
    let mut src = vec![];
    let result = v1.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 1);
    assert_eq!(src[0], 0x01);
}

#[test]
fn test_enum_decode() {
    let data = [0x01];

    let mut buf = Cursor::new(data);

    let result = EnumNoExprTest::decode_from(&mut buf, 0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val, EnumNoExprTest::B);

    let data = [0x00];

    let mut buf = Cursor::new(data);

    let result = EnumNoExprTest::decode_from(&mut buf, 0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val, EnumNoExprTest::A);
}

#[derive(Encoder, Decoder, Eq, PartialEq, Debug)]
#[repr(u8)]
#[derive(Default)]
pub enum EnumExprTest {
    #[fluvio(tag = 5)]
    #[default]
    D = 5,
    #[fluvio(tag = 10)]
    E = 10,
}

#[test]
fn test_enum_expr_encode() {
    let v1 = EnumExprTest::D;
    let mut src = vec![];
    let result = v1.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 1);
    assert_eq!(src[0], 0x05);
}

#[test]
fn test_enum_expr_decode() {
    let data = [0x05];

    let mut buf = Cursor::new(data);

    let result = EnumExprTest::decode_from(&mut buf, 0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val, EnumExprTest::D);
}

#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum WideEnum {
    #[fluvio(tag = 5)]
    #[default]
    D = 5,
    #[fluvio(tag = 10)]
    E = 10,
}

#[test]
fn test_wide_encode() {
    let v1 = WideEnum::D;
    let mut src = vec![];
    let result = v1.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 2);
    assert_eq!(v1.write_size(0), 2);
}

#[test]
fn test_try_decode() {
    let val: u16 = 10;
    let e: WideEnum = val.try_into().expect("convert");
    assert_eq!(e, WideEnum::E);
}

#[derive(Encoder, Decoder, Eq, PartialEq, Debug)]
pub enum GlColor {
    #[fluvio(tag = 1)]
    GlTextureRedType = 0x8C10,
    #[fluvio(tag = 0)]
    GlTextureGreenType = 0x8C11,
    #[fluvio(tag = 2)]
    GlTextureBlueType = 0x8C12,
}

impl Default for GlColor {
    fn default() -> Self {
        Self::GlTextureRedType
    }
}

#[test]
fn test_gl_colors() {
    let green = GlColor::GlTextureGreenType;
    let blue = GlColor::GlTextureBlueType;
    let mut dest = vec![];
    let result = green.encode(&mut dest, 0);
    assert!(result.is_ok());
    let result = blue.encode(&mut dest, 0);
    assert!(result.is_ok());
    assert_eq!(dest.len(), 2);
    assert_eq!(blue.write_size(0), 1);
    assert_eq!(dest[0], 0);
    assert_eq!(dest[1], 2);
}

#[derive(Encoder, Decoder, PartialEq, Debug)]
#[fluvio(encode_discriminant)]
enum EvenOdd {
    Even = 2,
    Odd = 1,
}
impl Default for EvenOdd {
    fn default() -> Self {
        Self::Even
    }
}

#[test]
fn test_encode_discriminant() {
    let even = EvenOdd::Even;
    let odd = EvenOdd::Odd;
    let mut dest = vec![];
    let result = even.encode(&mut dest, 0);
    assert!(result.is_ok());
    let result = odd.encode(&mut dest, 0);
    assert!(result.is_ok());
    assert_eq!(dest.len(), 2);
    assert_eq!(even.write_size(0), 1);
    assert_eq!(odd.write_size(0), 1);
    assert_eq!(dest[0], 2);
    assert_eq!(dest[1], 1);
}

#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[repr(u16)]
pub enum TestWideEnum {
    Echo = 1000,
    Status = 1001,
}
impl Default for TestWideEnum {
    fn default() -> Self {
        Self::Echo
    }
}

#[test]
fn test_simple_conversion() {
    let key: u16 = 1000;
    let key_enum: TestWideEnum = key.try_into().expect("conversion");
    assert_eq!(key_enum, TestWideEnum::Echo);
}

#[repr(i16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum TestErrorCode {
    // The server experienced an unexpected error when processing the request
    UnknownServerError = -1,
    #[default]
    None = 0,
}

#[test]
fn test_error_code_from_conversion2() {
    let val: i16 = 0;
    let error_code: TestErrorCode = val.try_into().expect("convert");
    assert_eq!(error_code, TestErrorCode::None);
}
