use std::io::Cursor;

use fluvio_protocol::derive::Decode;
use fluvio_protocol::derive::Encode;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

#[derive(Encode, Decode, Default, Debug)]
struct TestRecord {
    value: i8,
    #[fluvio(min_version = 1, max_version = 1)]
    value2: i8,
    #[fluvio(min_version = 1)]
    value3: i8,
}

#[test]
fn test_encode_version() {
    flv_util::init_logger();
    let mut record = TestRecord::default();
    record.value2 = 10;
    record.value3 = 5;

    // version 0 should only encode value
    let mut dest = vec![];
    record.encode(&mut dest, 0).expect("encode");
    assert_eq!(dest.len(), 1);
    assert_eq!(record.write_size(0), 1);

    // version 1 should encode value1,value2,value3
    let mut dest = vec![];
    record.encode(&mut dest, 1).expect("encode");
    assert_eq!(dest.len(), 3);
    assert_eq!(record.write_size(1), 3);

    // version 3 should only encode value, value3
    let mut dest = vec![];
    record.encode(&mut dest, 2).expect("encode");
    assert_eq!(dest.len(), 2);
    assert_eq!(dest[1], 5);
    assert_eq!(record.write_size(2), 2);
}

#[test]
fn test_decode_version() {
    // version 0 record
    let data = [0x08];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 0).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 0); // default

    let data = [0x08];
    assert!(
        TestRecord::decode_from(&mut Cursor::new(&data), 1).is_err(),
        "version 1 needs 3 bytes"
    );

    let data = [0x08, 0x01, 0x05];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 1).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 1);
    assert_eq!(record.value3, 5);

    let data = [0x08, 0x01, 0x05];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 3).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 0);
    assert_eq!(record.value3, 1); // default, didn't consume
}
