use super::SmokeTestCase;
use fluvio_test_util::test_meta::environment::EnvDetail;

const VALUE: u8 = 65;

/// each message has prefix
fn generate_pre_fix(topic: &str, offset: i64) -> String {
    //  format!("{}:{}", topic, offset)
    format!("topic-{topic} offset: {offset}")
}

/// generate test data based on iteration and option
///
#[allow(clippy::all)]
pub fn generate_message(offset: i64, test_case: &SmokeTestCase) -> Vec<u8> {
    let producer_record_size = test_case.option.producer_record_size as usize;

    let mut bytes = Vec::with_capacity(producer_record_size);

    let mut prefix = generate_pre_fix(test_case.environment.base_topic_name().as_str(), offset)
        .as_bytes()
        .to_vec();
    bytes.append(&mut prefix);

    // then fill int the dummy test data
    for _ in 0..producer_record_size {
        bytes.push(VALUE);
    }

    bytes
}

/// validate the message for given offset
#[allow(clippy::needless_range_loop)]
pub fn validate_message(iter: u32, offset: i64, test_case: &SmokeTestCase, data: &[u8]) {
    let prefix_string = generate_pre_fix(test_case.environment.base_topic_name().as_str(), offset);
    let prefix = prefix_string.as_bytes().to_vec();
    let prefix_len = prefix.len();

    let producer_record_size = test_case.option.producer_record_size as usize;

    let message_len = producer_record_size + prefix_len;
    assert_eq!(data.len(), message_len, "message should be: {message_len}");

    // check prefix
    for i in 0..prefix_len {
        assert!(
            data[i] == prefix[i],
            "prefix failed, iter: {}, index: {}, data: {}, prefix: {}, data len: {}, offset: {}, topic: {}, full_data: {}",
            iter,
            i,
            data[i],
            prefix_string,
            data.len(),
            offset,
            test_case.environment.base_topic_name().as_str(),
            std::str::from_utf8(data).expect("failed to parse"),
        );
    }

    // verify payload
    for i in 0..producer_record_size {
        assert!(
            data[i + prefix_len] == VALUE,
            "data not equal, offset: {}, topic: {}",
            offset,
            test_case.environment.base_topic_name().as_str()
        );
    }
}
