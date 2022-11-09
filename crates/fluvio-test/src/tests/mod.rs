pub mod self_test;
pub mod smoke;
pub mod concurrent;
pub mod multiple_partitions;
pub mod longevity;
pub mod producer;
pub mod consumer;
pub mod election;
pub mod producer_fail;
pub mod reconnection;
pub mod batching;
pub mod data_generator;
pub mod expected_fail;
pub mod expected_pass;
pub mod expected_timeout;
pub mod expected_fail_join_fail_first;
pub mod expected_fail_join_success_first;
// pub mod stats;

use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use crc::{Crc, CRC_32_CKSUM};

pub struct TestRecordBuilder {
    /// The producer will set this timestamp
    pub timestamp: SystemTime,
    pub tag: String,
    pub data: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestRecord {
    pub timestamp: SystemTime,
    pub tag: String,
    pub data: String,
    pub crc: u32,
}

impl TestRecord {
    pub fn validate_crc(&self) -> bool {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();

        // Use all fields to build CRC
        // Order is important: go by order of fields
        digest.update(format!("{:?}", &self.timestamp).as_bytes());
        digest.update(self.tag.as_bytes());
        digest.update(self.data.as_bytes());

        digest.finalize() == self.crc
    }
}

impl Default for TestRecordBuilder {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            tag: format!("{}", 0),
            data: String::new(),
        }
    }
}

impl TestRecordBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    // This is used more as a tag. This just needs to be Display
    pub fn with_tag(mut self, tag: String) -> Self {
        self.tag = tag;
        self
    }

    pub fn with_data(mut self, data: String) -> Self {
        self.data = data;
        self
    }

    pub fn with_random_data(mut self, data_size: usize) -> Self {
        self.data = Self::random_data(data_size);
        self
    }

    pub fn build(&self) -> TestRecord {
        TestRecord {
            timestamp: self.timestamp,
            tag: self.tag.clone(),
            data: self.data.clone(),
            crc: self.compute_data_crc(),
        }
    }

    fn random_data(data_size: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                                abcdefghijklmnopqrstuvwxyz\
                                0123456789)(*&^%$#@!~";

        let data: String = (0..data_size)
            .map(|_| {
                let idx = fastrand::usize(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();

        data
    }

    fn compute_data_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();

        // Use all fields to build CRC
        // Order is important: go by order of fields
        digest.update(format!("{:?}", &self.timestamp).as_bytes());
        digest.update(self.tag.as_bytes());
        digest.update(self.data.as_bytes());
        digest.finalize()
    }
}
