use std::{
    time::{Duration, SystemTime},
};

use serde::{Serialize, Deserialize};

use csv::ReaderBuilder;

// I think we want to move this entire file into the test driver.
// Tests will have to provide info for message generation/validation

use super::SmokeTestCase;

// This value is ascii Capital A
const VALUE: u8 = 65;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMessage {
    pub timestamp: u128,
    pub topic_name: String,
    pub offset: i64,
    pub length: usize,
    pub data: String,
}

impl TestMessage {
    pub fn with_offset(&mut self, offset: i64) {
        self.offset = offset;
    }

    /// generate test data based on iteration and option
    ///
    pub fn generate_message(offset: i64, test_case: &SmokeTestCase) -> Self {
        let producer_record_size = test_case.option.producer_record_size as usize;

        // TODO: Support reading in a payload from file here, but default to generating the message

        let data = vec![VALUE; producer_record_size];

        TestMessage {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Timestamp")
                .as_nanos(),
            topic_name: test_case.environment.topic_name.clone(),
            offset,
            length: producer_record_size,
            data: String::from_utf8(data).expect("Convert Vec<u8> to string"),
        }
    }

    /// validate the message for given offset
    #[allow(clippy::result_unit_err)]
    pub fn validate_message(
        _iter: u16,
        offset: i64,
        test_case: &SmokeTestCase,
        data: &[u8],
    ) -> Result<Self, ()> {
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(data);

        let mut iter = rdr.deserialize();

        let record: TestMessage = if let Some(result) = iter.next() {
            result.expect("Message format validation failed")
        } else {
            panic!("Expected a csv record, but didn't get one")
        };

        // Verify topic
        assert!(test_case.environment.topic_name == record.topic_name);

        // This gets messy w/ multiple producers or multiple partitions
        // FIXME: Topics w/ multiple partitions created externally will fail this check
        if test_case.environment.producers == 1 && test_case.environment.partition == 1 {
            // Verify offset
            assert!(
                offset == record.offset,
                "Got: {}, expected: {}",
                offset,
                record.offset
            );
        }

        let producer_record_size = test_case.option.producer_record_size as usize;

        // Check on data
        assert!(producer_record_size == record.length);
        assert!(producer_record_size == record.data.len());

        Ok(record)
    }

    // Return Duration between self.timestamp and now
    pub fn elapsed(self) -> Duration {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("timestamp")
            .as_nanos();

        Duration::from_nanos((now - self.timestamp) as u64)
    }
}
