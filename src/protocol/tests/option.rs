use std::io::Cursor;

use fluvio_protocol_core::{Decoder, Encoder};
use fluvio_protocol_derive::{Decode, Encode};

#[derive(Encode, Default, Decode, Debug)]
pub struct Parent {
    child: Option<Child>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct Child {
    flag: bool,
}

#[test]
fn test_encode() {
    let mut v1 = Parent::default();
    let mut child = Child::default();
    child.flag = true;
    v1.child = Some(child);
    let mut src = vec![];
    let result = v1.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 2);
    assert_eq!(src[0], 0x01);
    assert_eq!(src[1], 0x01);
}

#[test]
fn test_decode() {
    let data = [0x01, 0x01];

    let mut buf = Cursor::new(data);

    let result = Parent::decode_from(&mut buf, 0);
    assert!(result.is_ok());
    let val = result.unwrap();
    assert!(val.child.is_some());
}
