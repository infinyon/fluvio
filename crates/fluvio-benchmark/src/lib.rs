use std::{
    hash::{Hasher, Hash},
    sync::Arc,
};

use fluvio::RecordKey;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::hash_map::DefaultHasher;

pub mod consumer;
pub mod benchmark_config;
pub mod producer_worker;

pub struct BenchmarkRecord {
    pub key: RecordKey,
    pub data: String,
    pub hash: u64,
}

impl BenchmarkRecord {
    pub fn new(key: RecordKey, data: String) -> Self {
        let hash = hash_record(&key, &data);
        Self { key, data, hash }
    }
}

pub fn hash_record(key: &RecordKey, data: &str) -> u64 {
    let mut hasher_state = DefaultHasher::new();
    key.hash(&mut hasher_state);
    data.hash(&mut hasher_state);
    hasher_state.finish()
}

pub enum BenchmarkError {
    ErrorWithExplanation(&'static str),
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
