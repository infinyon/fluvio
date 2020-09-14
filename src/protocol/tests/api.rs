use std::io::Cursor;

use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::derive::KfDefault;
use kf_protocol::derive::RequestApi;
use kf_protocol::Decoder;
use kf_protocol::Encoder;

#[derive(Encode, Decode, KfDefault, RequestApi, Debug)]
#[fluvio_kf(
    api_min_version = 5,
    api_max_version = 6,
    api_key = 10,
    response = "TestResponse"
)]
pub struct TestRequest {
    pub value: i8,

    #[fluvio_kf(min_version = 1, max_version = 1)]
    pub value2: i8,

    #[fluvio_kf(min_version = 1, default = "-1")]
    pub value3: i8,
}

#[derive(Encode, Decode, KfDefault, Debug)]
pub struct TestResponse {
    pub value: i8,

    #[fluvio_kf(min_version = 1, max_version = 1)]
    pub value2: i8,

    #[fluvio_kf(min_version = 1)]
    pub value3: i8,
}

#[derive(Encode, Decode, KfDefault, Debug)]
pub struct KfMetadataResponse {
    #[fluvio_kf(min_version = 2)]
    pub cluster_id: Option<String>,
}

struct RandomStruct {}

impl RandomStruct {
    pub fn _type(&self) -> bool {
        true
    }
}

#[test]
fn test_metadata() {
    let d = KfMetadataResponse::default();
    assert_eq!(d.cluster_id, None);
}

#[test]
fn test_type() {
    let r = RandomStruct {};
    assert_eq!(r._type(), true);
}

#[test]
fn test_derive_api_version() {
    let mut record = TestRequest::default();
    record.value2 = 10;
    record.value3 = 5;

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
    let mut record = TestRequest::default();

    record.value2 = 10;
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
