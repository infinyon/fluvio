use crate::TestOption;

const VALUE: u8 = 65;

/// each message has prefix
fn generate_pre_fix(topic: &str, offset: i64) -> String {
    //  format!("{}:{}", topic, offset)
    format!("topic-{} offset: {}", topic, offset)
}

/// generate test data based on iteration and option
///
#[allow(clippy::all)]
pub fn generate_message(offset: i64, topic: &str, option: &TestOption) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(option.produce.record_size);

    let mut prefix = generate_pre_fix(topic, offset).as_bytes().to_vec();
    bytes.append(&mut prefix);

    // then fill int the dummy test data
    for _ in 0..option.produce.record_size {
        bytes.push(VALUE);
    }

    bytes
}

/// validate the message for given offset
#[allow(clippy::needless_range_loop)]
pub fn validate_message(iter: u16, offset: i64, topic: &str, option: &TestOption, data: &[u8]) {
    let prefix_string = generate_pre_fix(topic, offset);
    let prefix = prefix_string.as_bytes().to_vec();
    let prefix_len = prefix.len();

    let message_len = option.produce.record_size + prefix_len;
    assert_eq!(
        data.len(),
        message_len,
        "message should be: {}",
        message_len
    );

    // check prefix
    for i in 0..prefix_len {
        assert!(
            data[i] == prefix[i],
            "prefix failed, iter: {}, index: {}, data: {}, prefix: {}, data len: {}, offset: {}, topic: {}",
            iter,
            i,
            data[i],
            prefix_string,
            data.len(),
            offset,
            topic
        );
    }

    // verify payload
    for i in 0..option.produce.record_size {
        assert!(
            data[i + prefix_len] == VALUE,
            "data not equal, offset: {}, topic: {}",
            offset,
            topic
        );
    }
}
