use std::io::Error;
use std::io::Error as IoError;
use std::io::Cursor;
use std::io::ErrorKind;
use std::convert::TryInto;

use kf_protocol::bytes::Buf;
use kf_protocol::bytes::BufMut;
use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;
use kf_protocol::Encoder;
use kf_protocol::Decoder;
use kf_protocol::Version;


// manual encode 
pub enum Mix {
    A = 2,
    C = 3
}

impl Encoder for Mix {

    fn write_size(&self,_version: Version) -> usize {
            match self {
                Mix::A => 2,
                Mix::C => 2,
            }
        }

    fn encode<T>(&self, src: &mut T,version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            match self {
                Mix::A => {
                    let val = 2 as u8;
                    val.encode(src,version)?;
                },
                Mix::C => {
                    let val = 3 as u8;
                    val.encode(src,version)?;
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

     fn decode<T>(&mut self, src: &mut T,version: Version) -> Result<(), Error>
    where
        T: Buf {

            let mut value: u8 = 0;
            value.decode(src,version)?;
            match value {
                2 => {
                    *self = Mix::A;
                }
                3 => {
                    *self = Mix::C;
                }
                _ => return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        format!("invalid value for Mix: {}",value)
                ))
            }

            Ok(())
        }
   
}




#[derive(Encode,Debug)] 
pub enum VariantEnum {
    A(u16),
    C(String)
}


#[test]
fn test_var_encode() {

    let v1 = VariantEnum::C("hello".to_string());
    let mut src = vec![];
    let result = v1.encode(&mut src,0);
    assert!(result.is_ok());
    assert_eq!(src.len(),7);
    assert_eq!(v1.write_size(0),7);
 
}



/*
impl Encoder for VariantEnum {

    fn write_size(&self) -> usize {
            match self {
                VariantEnum::A(val) => val.write_size(),
                VariantEnum::C(val) => val.write_size(),
            }
        }

    fn encode<T>(&self, src: &mut T) -> Result<(), IoError>
        where
            T: BufMut,
        {
            match self {
                VariantEnum::A(val) => val.encode(src),
                VariantEnum::C(val) => val.encode(src)
            }
            
        }
}
*/

/*
impl Decoder for Mix {

     fn decode<T>(&mut self, src: &mut T) -> Result<(), Error>
    where
        T: Buf {

            let mut value: u8 = 0;
            value.decode(src)?;
            match value {
                2 => {
                    *self = Mix::A;
                }
                3 => {
                    *self = Mix::C;
                }
                _ => return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        format!("invalid value for Mix: {}",value)
                ))
            }

            Ok(())
        }
   
}
*/



#[derive(Encode,PartialEq,Decode,Debug)]
#[repr(u8)]
pub enum EnumNoExprTest {
    A,
    B
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
    let result = v1.encode(&mut src,0);
    assert!(result.is_ok());
    assert_eq!(src.len(),1);
    assert_eq!(src[0],0x01);
    
}


#[test]
fn test_enum_decode() {

    let data = [
        0x01
    ];

    let mut buf = Cursor::new(data);

    let result = EnumNoExprTest::decode_from(&mut buf,0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val,EnumNoExprTest::B); 

    let data = [
        0x00
    ];

    let mut buf = Cursor::new(data);

    let result = EnumNoExprTest::decode_from(&mut buf,0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val,EnumNoExprTest::A); 

}



#[derive(Encode,Decode,PartialEq,Debug)]
#[repr(u8)]
pub enum EnumExprTest {
    D = 5,
    E = 10
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
    let result = v1.encode(&mut src,0);
    assert!(result.is_ok());
    assert_eq!(src.len(),1);
    assert_eq!(src[0],0x05);
    
}



#[test]
fn test_enum_expr_decode() {

    let data = [
        0x05
    ];

    let mut buf = Cursor::new(data);

    let result = EnumExprTest::decode_from(&mut buf,0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert_eq!(val,EnumExprTest::D); 
}




#[derive(Encode,Decode,PartialEq,Debug)]
#[repr(u16)]
pub enum WideEnum {
    D = 5,
    E = 10
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
    let result = v1.encode(&mut src,0);
    assert!(result.is_ok());
    assert_eq!(src.len(),2);
    assert_eq!(v1.write_size(0),2);
}

#[test]
fn test_try_decode() {
    let val: u16 = 10;
    let e: WideEnum = val.try_into().expect("convert");
    assert_eq!(e,WideEnum::E);
}

