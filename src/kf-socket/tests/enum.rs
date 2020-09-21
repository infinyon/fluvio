use std::convert::TryInto;

use fluvio_protocol::derive::{Decode, Encode};

#[fluvio(encode_discriminant)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[repr(u16)]
pub enum TestKafkaApiEnum {
    Echo = 1000,
    Status = 1001,
}

impl Default for TestKafkaApiEnum {
    fn default() -> TestKafkaApiEnum {
        TestKafkaApiEnum::Echo
    }
}

#[test]
fn test_conversion() {
    let key: u16 = 1000;
    let key_enum: TestKafkaApiEnum = key.try_into().expect("conversion");
    assert_eq!(key_enum, TestKafkaApiEnum::Echo);
}
