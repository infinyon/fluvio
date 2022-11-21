use std::hash::{Hasher, Hash};

use async_std::{
    channel::{SendError, RecvError},
    future::TimeoutError,
};
use fluvio::{RecordKey, FluvioError};
use rand::{distributions::Alphanumeric, Rng};
use std::collections::hash_map::DefaultHasher;

pub mod consumer;
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

#[derive(Debug, Clone)]
pub enum BenchmarkError {
    ErrorWithExplanation(String),
    Timeout,
    /// Failed to Send or Recv from a channel
    ChannelSendRecv,
    FluvioError(String),
}

impl<T> From<SendError<T>> for BenchmarkError {
    fn from(_: SendError<T>) -> Self {
        BenchmarkError::ChannelSendRecv
    }
}

impl From<RecvError> for BenchmarkError {
    fn from(_: RecvError) -> Self {
        BenchmarkError::ChannelSendRecv
    }
}

impl From<TimeoutError> for BenchmarkError {
    fn from(_: TimeoutError) -> Self {
        BenchmarkError::Timeout
    }
}

impl From<FluvioError> for BenchmarkError {
    fn from(e: FluvioError) -> Self {
        BenchmarkError::FluvioError(format!("fluvio error {:?}", e))
    }
}

fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}
