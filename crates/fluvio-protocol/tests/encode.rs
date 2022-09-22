use fluvio_protocol::{Encoder, EncoderVarInt};

#[derive(Encoder, Default, Debug)]
pub struct SimpleRecord {
    #[varint]
    len: i64,
    attributes: i8,
}

#[derive(Encoder, Default, Debug)]
pub struct RecordSet {
    records: Vec<SimpleRecord>,
}

impl RecordSet {
    fn add_record(&mut self, record: SimpleRecord) {
        self.records.push(record);
    }
}

#[test]
fn test_encode_recordset() {
    let mut recordset = RecordSet::default();
    let record = SimpleRecord {
        attributes: 10,
        len: 4,
    };
    recordset.add_record(record);

    let mut src = vec![];
    let result = recordset.encode(&mut src, 0);
    assert!(result.is_ok());
    assert_eq!(src.len(), 6);
    assert_eq!(src[5], 0x0a);
    assert_eq!(recordset.write_size(0), src.len());
}
