use std::io::Cursor;
use std::io::Error;

use fluvio_protocol::{Decoder, Encoder};

#[derive(Encoder, Default, Decoder, Debug)]
pub struct TupleStruct(String, i32);

#[test]
fn test_encode_tuple_struct() -> Result<(), Error> {
    let v1 = TupleStruct {
        0: "Struct without named fields".to_string(),
        1: 42,
    };
    let mut src = vec![];
    v1.encode(&mut src, 0)?;

    // 29 bytes (String) + 4 bytes (i32)
    assert_eq!(src.len(), 33);
    let v2 = TupleStruct::decode_from(&mut Cursor::new(src), 0)?;
    assert_eq!(v2.0, "Struct without named fields");
    assert_eq!(v2.1, 42);

    Ok(())
}
