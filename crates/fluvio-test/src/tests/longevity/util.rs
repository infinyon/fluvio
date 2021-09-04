#![allow(dead_code)]
use md5::Digest;
use serde::{Serialize, Deserialize};
use std::time::SystemTime;

type Record = Vec<u8>;

#[derive(Debug, Serialize, Deserialize)]
pub struct LongevityRecord {
    /// The producer will set this timestamp
    pub timestamp: SystemTime,
    pub testrun_offset: u32,
    pub data: Vec<u8>,
}

impl Default for LongevityRecord {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            testrun_offset: 0,
            data: Vec::new(),
        }
    }
}

impl LongevityRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.testrun_offset = offset;
        self
    }

    pub fn with_data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }
}

// TODO: Data size needs to be configurable
pub fn rand_printable_record() -> Record {
    use rand::Rng;

    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";
    const DATA_SIZE: usize = 1000;
    let mut rng = rand::thread_rng();

    let data: String = (0..DATA_SIZE)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    // TODO: Return serialized LongevityRecord
    data.as_bytes().to_vec()
}

pub fn hash_messages(messages: &[String]) -> String {
    let mut hasher = md5::Md5::new();
    for m in messages.iter() {
        hasher.update(m);
    }
    format!("{:X?}", hasher.finalize())
}

pub fn hash_record(record: &[u8]) -> String {
    format!("{:X}", md5::Md5::digest(record))
}
