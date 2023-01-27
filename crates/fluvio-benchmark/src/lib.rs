use std::hash::{Hasher, Hash};
use std::collections::hash_map::DefaultHasher;
use std::io::Error as IoError;

use async_channel::{SendError, RecvError};

use rand::{distributions::Alphanumeric, Rng};
use fluvio_future::future::TimeoutError;
use fluvio::{RecordKey, FluvioError};

pub mod consumer_worker;
pub mod benchmark_config;
pub mod producer_worker;
pub mod stats_collector;
pub mod benchmark_driver;
pub mod stats;

pub struct BenchmarkRecord {
    pub key: RecordKey,
    pub data: String,
    pub hash: u64,
}

impl BenchmarkRecord {
    pub fn new(key: RecordKey, data: String) -> Self {
        let hash = hash_record(&data);
        Self { key, data, hash }
    }
}

pub fn hash_record(data: &str) -> u64 {
    let mut hasher_state = DefaultHasher::new();
    data.hash(&mut hasher_state);
    hasher_state.finish()
}

#[derive(thiserror::Error, Debug)]
pub enum BenchmarkError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("{0}")]
    ErrorWithExplanation(String),
    #[error(transparent)]
    TimeoutError(#[from] TimeoutError),
    #[error("SendError")]
    SendError,
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error(transparent)]
    FluvioError(#[from] FluvioError),
}

impl<T> From<SendError<T>> for BenchmarkError {
    fn from(_: SendError<T>) -> Self {
        BenchmarkError::SendError
    }
}
fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}
