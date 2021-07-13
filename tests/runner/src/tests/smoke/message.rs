use std::{
    time::{Duration, SystemTime},
};

use serde::{Serialize, Deserialize};

// I think we want to move this entire file into the test driver.
// Tests will have to provide info for message generation/validation

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMessage {
    pub timestamp: u64,
    pub topic_name: String,
    pub offset: i64,
    pub length: u64,
    pub checksum: String, // This is just an idea? Possibly Might need to think about TestMessage as an Enum? To support random data validation too. Just a thought.
}

impl TestMessage {
    pub fn with_offset(&mut self, offset: i64) {
        self.offset = offset;
    }

    pub fn generate_message(self) -> Vec<u8> {
        // timestamp
        // topic name
        // offset
        // length
        // padding
        Vec::new()
    }

    pub fn validate_message(_message: Vec<u8>) -> Result<Self, ()> {
        // After validating return an instantiated TestMessage
        Ok(Self::default())
    }

    // Return Duration between self.timestamp and now
    pub fn elapsed(self) -> Duration {
        unimplemented!()
    }
}

// I'm not sure if this is going to work the way I need it to
// What if the validation fails? I can't return a Result. I'll have to panic.
impl From<Vec<u8>> for TestMessage {
    fn from(_msg: Vec<u8>) -> Self {
        TestMessage {
            timestamp: 0,
            topic_name: String::from(""),
            offset: 0,
            length: 0,
            checksum: String::from(""),
        }
    }
}

use super::SmokeTestCase;

// This value is ascii Capital A
const VALUE: u8 = 65;

// Maybe let's comma-separate?
/// each message has prefix
fn generate_pre_fix(topic: &str, offset: i64) -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    //  format!("{}:{}", topic, offset)
    format!("{},topic:{},offset:{},", now, topic, offset)
}

/// generate test data based on iteration and option
///
/// Message format:
/// <current host time> <
#[allow(clippy::all)]
pub fn generate_message(offset: i64, test_case: &SmokeTestCase) -> Vec<u8> {
    let producer_record_size = test_case.option.producer_record_size as usize;

    let mut bytes = Vec::with_capacity(producer_record_size);

    // TODO: Support reading in a payload from file here, but default to generating the message

    // What is this prefix used for?
    // Maybe timestamp can go here
    let mut prefix = generate_pre_fix(test_case.environment.topic_name.as_str(), offset)
        .as_bytes()
        .to_vec();
    bytes.append(&mut prefix);

    let message_padding = vec![VALUE; producer_record_size];

    bytes.extend(message_padding);
    //// Is there a simpler way to do this? The VALUE confused me a bit.
    //// then fill int the dummy test data
    //for _ in 0..producer_record_size {
    //    bytes.push(VALUE);
    //}

    bytes
}

/// validate the message for given offset
#[allow(clippy::needless_range_loop)]
pub fn validate_message(_iter: u16, offset: i64, test_case: &SmokeTestCase, data: &[u8]) {
    // We can split data up w/ `,`. There
    // timestamp,topic,offset,padding

    let prefix_string = generate_pre_fix(test_case.environment.topic_name.as_str(), offset);
    let prefix_split: Vec<&str> = prefix_string.split_terminator(",").collect();
    let prefix = prefix_string.as_bytes().to_vec();
    let prefix_len = prefix.len();

    let producer_record_size = test_case.option.producer_record_size as usize;

    // edge case: prefix_len drifts bc of the length of timestamp
    let message_len = producer_record_size + prefix_len;
    assert_eq!(
        data.len(),
        message_len,
        "message should be: {}",
        message_len
    );

    // FIXME: Ignore the timestamp, but verify the topic name and offset
    let data_str = String::from_utf8(data.to_vec()).unwrap();
    let data_vec: Vec<&str> = data_str.split_terminator(",").collect();

    // check prefix
    // Verify topic matches
    assert!(
        data_vec[1] == prefix_split[1],
        "topic verification failed. Got {} expected {}",
        data_vec[1],
        prefix_split[1]
    );

    // Verify offset matches
    assert!(
        data_vec[2] == prefix_split[2],
        "offset verification failed. Got {} expected {}",
        data_vec[2],
        prefix_split[2]
    );

    // verify payload
    let expected_msg_padding = String::from_utf8(vec![VALUE; producer_record_size]).unwrap();

    assert!(
        data_vec[3] == expected_msg_padding,
        "payload verification failed. Got {} expected {}",
        data_vec[3],
        expected_msg_padding
    );
}
