use std::io::Cursor;

use fluvio_protocol::api::Request;
use fluvio_protocol::{Decoder, Encoder, FluvioDefault, RequestApi};

#[derive(Encoder, Decoder, FluvioDefault, RequestApi, Debug)]
#[fluvio(
    api_min_version = 5,
    api_max_version = 6,
    api_key = 10,
    response = "TestResponse"
)]
pub struct TestRequest {
    pub value: i8,

    #[fluvio(min_version = 1, max_version = 1)]
    pub value2: i8,

    #[fluvio(min_version = 1, default = "-1")]
    pub value3: i8,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct TestResponse {
    pub value: i8,

    #[fluvio(min_version = 1, max_version = 1)]
    pub value2: i8,

    #[fluvio(min_version = 1)]
    pub value3: i8,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct KfMetadataResponse {
    #[fluvio(min_version = 2)]
    pub cluster_id: Option<String>,
}

struct RandomStruct {}

impl RandomStruct {
    pub fn _type(&self) -> bool {
        true
    }
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct MyAddr {
    pub value: i8,

    // only works up to version 4
    #[fluvio(max_version = 4)]
    pub addr: i8,

    // works from version 2
    #[fluvio(min_version = 2)]
    pub name: i8,

    // only works from version 3 to version 9
    #[fluvio(min_version = 4, max_version = 9)]
    pub addr2: i8,
}

#[test]
fn test_metadata() {
    let d = KfMetadataResponse::default();
    assert_eq!(d.cluster_id, None);
}

#[test]
fn test_type() {
    let r = RandomStruct {};
    assert!(r._type());
}

#[test]
fn test_derive_api_version() {
    let record = TestRequest {
        value2: 10,
        value3: 5,
        ..Default::default()
    };

    // version 0 should only encode value
    let mut dest = vec![];
    record.encode(&mut dest, 0).expect("encode");
    assert_eq!(dest.len(), 1);

    // version 1 should encode value1,value2,value3
    let mut dest = vec![];
    record.encode(&mut dest, 1).expect("encode");
    assert_eq!(dest.len(), 3);

    // version 3 should only encode value, value3
    let mut dest = vec![];
    record.encode(&mut dest, 2).expect("encode");
    assert_eq!(dest.len(), 2);
    assert_eq!(dest[1], 5);
}

#[test]
fn test_api_request() {
    assert_eq!(TestRequest::API_KEY, 10);
    assert_eq!(TestRequest::MIN_API_VERSION, 5);
    assert_eq!(TestRequest::MAX_API_VERSION, 6);
}

#[test]
fn test_api_getter() {
    let record = TestRequest {
        value2: 10,
        ..Default::default()
    };
    assert_eq!(record.value2, 10);
}

#[test]
fn test_decode_version() {
    // version 0 record
    let data = [0x08];
    let record = TestRequest::decode_from(&mut Cursor::new(&data), 0).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 0); // default

    let data = [0x08];
    assert!(
        TestRequest::decode_from(&mut Cursor::new(&data), 1).is_err(),
        "version 1 needs 3 bytes"
    );

    let data = [0x08, 0x01, 0x05];
    let record = TestRequest::decode_from(&mut Cursor::new(&data), 1).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 1);
    assert_eq!(record.value3, 5);

    let data = [0x08, 0x01, 0x05];
    let record = TestRequest::decode_from(&mut Cursor::new(&data), 3).expect("decode");
    assert_eq!(record.value, 8);
    assert_eq!(record.value2, 0);
    assert_eq!(record.value3, 1); // default, didn't consume
}

#[test]
fn test_encoding() {
    // version 0 should only encode value
    let mut dest = vec![];
    let addr1 = MyAddr {
        value: 1,
        name: 2,
        addr: 3,
        addr2: 4,
    };
    addr1.encode(&mut dest, 0).expect("encode");
    assert_eq!(dest.len(), 2); // only 2 should survive
}

#[test]
fn test_encoding_v3() {
    // version 0 should only encode value
    let mut dest = vec![];
    let addr1 = MyAddr {
        value: 1,
        name: 2,
        addr: 3,
        addr2: 4,
    };
    addr1.encode(&mut dest, 3).expect("encode");
    assert_eq!(dest.len(), 3); // only 2 should survive
}

#[test]
fn test_encoding_v5() {
    // version 0 should only encode value
    let mut dest = vec![];
    let addr1 = MyAddr {
        value: 1,
        name: 2,
        addr: 3,
        addr2: 4,
    };
    addr1.encode(&mut dest, 5).expect("encode");
    assert_eq!(dest.len(), 3); // only 2 should survive

    let record = MyAddr::decode_from(&mut Cursor::new(&dest), 5).expect("decode");
    assert_eq!(record.value, 1);
    assert_eq!(record.addr, 0); // didn't surive encoding
    assert_eq!(record.name, 2);
    assert_eq!(record.addr2, 4);
}

#[test]
fn test_encoding_v10() {
    // version 0 should only encode value
    let mut dest = vec![];
    let addr1 = MyAddr {
        value: 1,
        name: 2,
        addr: 3,
        addr2: 4,
    };
    addr1.encode(&mut dest, 10).expect("encode");
    assert_eq!(dest.len(), 2); // only 2 should survive

    let record = MyAddr::decode_from(&mut Cursor::new(&dest), 10).expect("decode");
    assert_eq!(record.value, 1);
    assert_eq!(record.addr, 0); // didn't surive encoding
    assert_eq!(record.name, 2);
    assert_eq!(record.addr2, 0);
}
