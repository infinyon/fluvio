#![allow(dead_code)]
use md5::Digest;
use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use crc::{Crc, CRC_32_CKSUM};

type Record = Vec<u8>;

pub struct LongevityRecordBuilder {
    /// The producer will set this timestamp
    pub timestamp: SystemTime,
    // Index of this record wrt the longevity session, starting at 0
    pub testrun_offset: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LongevityRecord {
    pub timestamp: SystemTime,
    pub offset: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub crc: u32,
}

impl LongevityRecord {
    pub fn validate_crc(&self) -> bool {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();

        // Use all fields to build CRC
        // Order is important: go by order of fields
        digest.update(format!("{:?}", &self.timestamp).as_bytes());
        digest.update(format!("{}", &self.offset).as_bytes());
        digest.update(&self.data);

        digest.finalize() == self.crc
    }
}

impl Default for LongevityRecordBuilder {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            testrun_offset: 0,
            data: Vec::new(),
        }
    }
}

impl LongevityRecordBuilder {
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

    pub fn with_random_data(mut self, data_size: usize) -> Self {
        self.data = Self::random_data(data_size);
        self
    }

    pub fn build(&self) -> LongevityRecord {
        LongevityRecord {
            timestamp: self.timestamp,
            offset: self.testrun_offset,
            data: self.data.clone(),
            crc: self.compute_data_crc(),
        }
    }

    fn random_data(data_size: usize) -> Vec<u8> {
        use rand::Rng;

        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                                abcdefghijklmnopqrstuvwxyz\
                                0123456789)(*&^%$#@!~";

        let mut rng = rand::thread_rng();

        let data: String = (0..data_size)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();

        data.as_bytes().to_vec()
    }

    fn compute_data_crc(&self) -> u32 {
        let crc = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();

        // Use all fields to build CRC
        // Order is important: go by order of fields
        digest.update(format!("{:?}", &self.timestamp).as_bytes());
        digest.update(format!("{}", &self.testrun_offset).as_bytes());
        digest.update(&self.data);
        digest.finalize()
    }
}

pub fn rand_printable_record(data_size: usize) -> Record {
    use rand::Rng;

    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";

    let mut rng = rand::thread_rng();

    let data: String = (0..data_size)
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
