use std::{
    hash::{Hasher, Hash},
    sync::Arc,
};

use fluvio::RecordKey;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::hash_map::DefaultHasher;

pub mod consumer;
pub mod consumer_worker;
pub mod benchmark_config;
pub mod producer_worker;
pub mod stats_collector;
pub mod benchmark_driver;

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
    WrappedErr(Arc<dyn std::fmt::Debug + Sync + Send>),
}
impl BenchmarkError {
    pub fn wrap_err(e: impl std::fmt::Debug + Sync + Send + 'static) -> BenchmarkError {
        BenchmarkError::WrappedErr(Arc::new(e))
    }
}

fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}



