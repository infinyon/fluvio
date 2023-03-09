use std::convert::TryInto;

use fluvio_protocol::derive::{Decoder, Encoder};

#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum TestKafkaApiEnum {
    #[default]
    Echo = 1000,
    Status = 1001,
}

#[test]
fn test_conversion() {
    let key: u16 = 1000;
    let key_enum: TestKafkaApiEnum = key.try_into().expect("conversion");
    assert_eq!(key_enum, TestKafkaApiEnum::Echo);
}
