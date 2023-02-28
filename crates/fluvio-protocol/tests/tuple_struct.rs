use std::io::Cursor;
use std::io::Error;

use fluvio_protocol::{Decoder, Encoder};

#[derive(Encoder, Default, Decoder, Debug)]
pub struct TupleStruct(String, i32);

#[derive(Encoder, Default, Decoder, Debug)]
pub struct TestString(String);

#[test]
fn test_encode_tuple_struct() -> Result<(), Error> {
    let v1 = TupleStruct("Struct without named fields".to_string(), 42);
    let mut src = vec![];
    v1.encode(&mut src, 0)?;

    // 29 bytes (String) + 4 bytes (i32)
    assert_eq!(src.len(), 33);
    let v2 = TupleStruct::decode_from(&mut Cursor::new(src), 0)?;
    assert_eq!(v2.0, "Struct without named fields");
    assert_eq!(v2.1, 42);

    Ok(())
}

#[test]
fn test_encode_test_string() -> Result<(), Error> {
    let v1 = TestString("Inner string".to_string());
    let mut src = vec![];
    v1.encode(&mut src, 0)?;

    // 14 bytes (String)
    assert_eq!(src.len(), 14);
    let v2 = TestString::decode_from(&mut Cursor::new(src), 0)?;
    assert_eq!(v2.0, "Inner string");

    Ok(())
}
