use std::io::Cursor;

use fluvio_protocol::{Decoder, Encoder};

const TEST_VERSION: i16 = 1;

fn test_version() -> i16 {
    1
}
#[derive(Encoder, Decoder, Default, Debug)]
struct TestRecord {
    value: i8,
    #[fluvio(min_version = 1, max_version = 1)]
    value2: i8,
    #[fluvio(min_version = 1)]
    value3: i8,
    #[fluvio(min_version = TEST_VERSION, max_version = "test_version()")]
    value4: i8,
    #[fluvio(min_version = -1, max_version = "TEST_VERSION")]
    value5: i8,
    #[fluvio(min_version = TEST_VERSION)]
    value6: i8,
}

#[test]
fn test_encode_version() {
    fluvio_future::subscriber::init_logger();
    let record = TestRecord {
        value2: 10,
        value3: 5,
        value4: 15,
        value5: 20,
        value6: 25,
        ..Default::default()
    };

    // version 0 should only encode value,value5
    let mut dest = vec![];
    record.encode(&mut dest, 0).expect("encode");
    assert_eq!(dest.len(), 2);
    assert_eq!(record.write_size(0), 2);

    // version 1 should encode value1,value2,value3,value4,value5,value6
    let mut dest = vec![];
    record.encode(&mut dest, 1).expect("encode");
    assert_eq!(dest.len(), 6);
    assert_eq!(record.write_size(1), 6);

    // version 3 should only encode value, value3, value6
    let mut dest = vec![];
    record.encode(&mut dest, 3).expect("encode");
    assert_eq!(dest.len(), 3);
    assert_eq!(dest[1], 5);
    assert_eq!(dest[2], 25);
    assert_eq!(record.write_size(2), 3);
}

#[test]
fn test_decode_version() {
    // version 0 record
    let data = [0x08, 0x01];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 0).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value5, 1);
    assert_eq!(record.value2, 0); // default

    let data = [0x08];
    assert!(
        TestRecord::decode_from(&mut Cursor::new(&data), 1).is_err(),
        "version 1 needs 6 bytes"
    );

    let data = [0x08, 0x01, 0x05, 0x06, 0x07, 0x09];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 1).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 1);
    assert_eq!(record.value3, 5);
    assert_eq!(record.value4, 6);
    assert_eq!(record.value5, 7);
    assert_eq!(record.value6, 9);

    let data = [0x08, 0x01, 0x05, 0x06];
    let record = TestRecord::decode_from(&mut Cursor::new(&data), 3).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 0);
    assert_eq!(record.value3, 1); // default, didn't consume
    assert_eq!(record.value6, 5);
}
