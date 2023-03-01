//use std::fmt::Debug;
use std::io::Cursor;

use fluvio_protocol::{Decoder, Encoder};

#[derive(Encoder)]
pub struct SimpleStructE<R> {
    value: R,
}

type SSByte = SimpleStructE<i8>;

#[test]
fn test_generic_with_bounds() {
    let record = SSByte { value: 3 };

    let mut src = vec![];
    record.encode(&mut src, 0).expect("encode");

    assert_eq!(src.len(), 1);
}

#[derive(Encoder)]
pub struct SimpleVec<R> {
    value: Vec<R>,
}

type SV8 = SimpleVec<i8>;

#[test]
fn test_generic_with_bounds_vec() {
    let record = SV8 { value: vec![3] };

    let mut src = vec![];
    record.encode(&mut src, 0).expect("encode");

    assert_eq!(src.len(), 5);
}

/*
#[derive(Encoder)]
#[fluvio(trace)]
pub struct SimpleStructWithTrace<R>
{
    value: R,
}

type SSByteTrace = SimpleStructWithTrace<u8>;

#[test]
fn test_generic_with_bounds_with_trace() {


    let record = SSByteTrace {
        value: 3
    };

    let mut src = vec![];
    let result = record.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 1);
}
*/

#[derive(Decoder, Default)]
pub struct SimpleStructD<R> {
    value: R,
}

type SSByteD = SimpleStructD<u8>;

#[test]
fn test_decoding_generic() {
    let src = vec![3];
    let d = SSByteD::decode_from(&mut Cursor::new(&src), 0).expect("decoded");
    assert_eq!(d.value, 3);
}

#[derive(Decoder, Default)]
pub struct SimpleVecD<R> {
    value: Vec<R>,
}

type _SVD8 = SimpleVecD<i8>;

/*
#[test]
fn test_decoding_generic_with_vec() {
    let mut src = vec![];
    src.push(3);
    let d = SVD8::decode_from(&mut Cursor::new(&src), 0).expect("decoded");
    //assert_eq!(d.value, 3);
}
*/

/*
#[derive(Encoder, Decoder, Default, Debug)]
pub struct GenericRecord<R>
where
    R: Encoder + Decoder + Debug,
{
    len: i64,
    value: R,
}

#[test]
fn test_generic() {
    let record = GenericRecord {
        len: 20,
        value: 25_i64,
    };

    let mut src = vec![];
    let result = record.encode(&mut src, 0);
    assert!(result.is_ok());

    assert_eq!(src.len(), 16);

    let result2 = GenericRecord::<i64>::decode_from(&mut Cursor::new(&src), 0);
    assert!(result2.is_ok());
    let decoded_record = result2.expect("is ok");
    assert_eq!(decoded_record.len, 20);
    assert_eq!(decoded_record.value, 25);
}
*/

#[derive(Default, Debug)]
pub struct Root<R> {
    pub topic: String,
    pub stream_id: u32,
    pub partition: Child<R>,
}

#[derive(Default, Debug)]
pub struct Child<R> {
    pub value: R,
}

#[derive(Default, Decoder, Debug)]
pub struct Root2<R> {
    pub topic: String,
    pub stream_id: u32,
    pub partition: Child2<R>,
}

#[derive(Default, Decoder, Debug)]
pub struct Child2<R> {
    pub value: R,
}
