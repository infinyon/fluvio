use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Error;

use fluvio_protocol_derive::{ Decode, Encode };
use fluvio_protocol_core::{ Decoder, Encoder };

#[derive(Encode, Default, Decode, Debug)]
pub struct MapHolder {
    values: BTreeMap<i32, Vec<i32>>,
}

#[test]
fn test_encode_treemap() -> Result<(), Error> {
    let mut v1 = MapHolder::default();
    v1.values.insert(1, vec![0, 2]); // 4 (key) + 4 (vec len) + 8  = 16
    v1.values.insert(5, vec![1]); //  4 (key) + 4 (vec len) + 4  = 12
    let mut src = vec![];
    v1.encode(&mut src, 0)?;
    assert_eq!(src.len(), 30);
    let v2 = MapHolder::decode_from(&mut Cursor::new(src), 0)?;
    assert_eq!(v2.values.len(), 2);
    let r1 = v2.values.get(&1).unwrap();
    assert_eq!(r1.len(), 2);
    assert_eq!(r1[0], 0);
    assert_eq!(r1[1], 2);
    let r2 = v2.values.get(&5).unwrap();
    assert_eq!(r2[0], 1);
    Ok(())
}
