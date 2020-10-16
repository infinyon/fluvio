
use crate::TestOption;

const VALUE: u8 = 65;

/// generate test data based on iteration and option
#[allow(clippy::all)]
pub fn generate_message(offset: i64, topic: &str, option: &TestOption) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(option.produce.record_size);

    let message = format!("{}:{}", topic, offset);
    for p in message.as_bytes() {
        bytes.push(*p);
    }
    for _ in 0..option.produce.record_size {
        bytes.push(VALUE);
    }

    bytes
}

/// validate the message
#[allow(clippy::needless_range_loop)]
pub fn validate_message(offset: i64, topic: &str, option: &TestOption, data: &[u8]) {
    let message = format!("{}:{}", topic, offset);
    let prefix = message.as_bytes();
    let prefix_len = prefix.len();

    assert_eq!(data.len(), option.produce.record_size + prefix_len);

    // check prefix
    for i in 0..prefix_len {
        assert!(data[i] == prefix[i],"prefix not equal,offset: {}, topic: {}",offset,topic);
    }

    for i in 0..option.produce.record_size {
        assert!(data[i + prefix_len] == VALUE, "data not equal, offset: {}, topic: {}",offset,topic);
    }
}
