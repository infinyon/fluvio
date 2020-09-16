use std::io::Cursor;

use fluvio_protocol_core::{Decoder, DecoderVarInt};
use fluvio_protocol_derive::Decode;

#[derive(Decode, Default, Debug)]
pub struct SimpleRecord {
    #[varint]
    len: i64,
    attributes: i8,
}

#[derive(Decode, Default, Debug)]
pub struct RecordSet {
    records: Vec<SimpleRecord>,
}

#[test]
fn test_decode_record() {
    let data = [
        0x14, // record length of 7
        0x04, // attributes
    ];

    let mut buf = Cursor::new(data);

    let result = SimpleRecord::decode_from(&mut buf, 0);
    assert!(result.is_ok());
    let record = result.unwrap();
    assert_eq!(record.len, 10);
    assert_eq!(record.attributes, 4);
}

#[test]
fn test_decode_recordset() {
    let data = [
        0x00, 0x00, 0x00, 0x01, // record count
        0x14, // record length of 7
        0x04, // attributes
    ];

    let result = RecordSet::decode_from(&mut Cursor::new(&data), 0);
    assert!(result.is_ok());
    let recordset = result.unwrap();
    let records = &recordset.records;
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.len, 10);
    assert_eq!(record.attributes, 4);
}
