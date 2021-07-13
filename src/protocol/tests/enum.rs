use std::convert::TryInto;
use std::io::Cursor;
use std::io::Error;
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_protocol_core::bytes::{Buf, BufMut};
use fluvio_protocol_core::{Decoder, Encoder, Version};
use fluvio_protocol_derive::{Decode, Encode};

// manual encode
pub enum Mix {
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

impl Default for Mix {
    fn default() -> Mix {
        Mix::A
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
                    format!("invalid value for Mix: {}", value),
                ))
            }
        }

        Ok(())
    }
}

#[derive(Encode, Debug)]
pub enum VariantEnum {
    A(u16),
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

#[derive(Encode, Debug)]
pub enum NamedEnum {
    Apple { seeds: u16 },
    Banana { peel: bool },
}

#[test]
fn test_named_encode() {
    let apple = NamedEnum::Apple { seeds: 13 };
    let mut dest = Vec::new();
    apple.encode(&mut dest, 0).unwrap();
    assert_eq!(dest.len(), 3);
    assert_eq!(dest[0], 0x00);
    assert_eq!(dest[1], 0x00);
    assert_eq!(dest[2], 0x0d);
}

#[derive(Encode, PartialEq, Decode, Debug)]
#[repr(u8)]
pub enum EnumNoExprTest {
    A,
    B,
}

impl Default for EnumNoExprTest {
    fn default() -> EnumNoExprTest {
        EnumNoExprTest::A
    }
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

#[derive(Encode, Decode, PartialEq, Debug)]
#[repr(u8)]
pub enum EnumExprTest {
    #[fluvio(tag = 5)]
    D = 5,
    #[fluvio(tag = 10)]
    E = 10,
}

impl Default for EnumExprTest {
    fn default() -> EnumExprTest {
        EnumExprTest::D
    }
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

#[derive(Encode, Decode, PartialEq, Debug)]
#[repr(u16)]
pub enum WideEnum {
    #[fluvio(tag = 5)]
    D = 5,
    #[fluvio(tag = 10)]
    E = 10,
}

impl Default for WideEnum {
    fn default() -> WideEnum {
        WideEnum::D
    }
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

#[derive(Encode, Decode, PartialEq, Debug)]
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

#[derive(Encode, Decode, PartialEq, Debug)]
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

#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
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
#[derive(PartialEq, Debug, Encode, Decode)]
#[fluvio(encode_discriminant)]
pub enum TestErrorCode {
    // The server experienced an unexpected error when processing the request
    UnknownServerError = -1,
    None = 0,
}

impl Default for TestErrorCode {
    fn default() -> Self {
        TestErrorCode::None
    }
}

#[test]
fn test_error_code_from_conversion2() {
    let val: i16 = 0;
    let erro_code: TestErrorCode = val.try_into().expect("convert");
    assert_eq!(erro_code, TestErrorCode::None);
}
