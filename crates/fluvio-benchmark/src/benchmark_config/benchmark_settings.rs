use std::time::Duration;

use fluvio::Compression;
use serde::{Serialize, Deserialize};

use super::benchmark_matrix::{RecordSizeStrategy, RecordKeyAllocationStrategy};
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkSettings {
    pub topic_name: String,
    /// Each sample is a collection of batches that all run on the same topic.
    pub worker_timeout: Duration,
    pub num_samples: u64,
    pub num_batches_per_sample: u64,
    pub duration_between_batches: Duration,
    pub num_records_per_producer_worker_per_batch: u64,
    pub producer_batch_size: u64,
    pub producer_queue_size: u64,
    pub producer_linger: Duration,
    pub producer_server_timeout: Duration,
    pub producer_compression: Compression,
    // TODO
    // pub producer_isolation:...,
    // TODO
    // pub producer_delivery_semantic,
    pub consumer_max_bytes: u64,
    // TODO
    // pub consumer_isolation:...,
    pub num_concurrent_producer_workers: u64,
    /// Total number of concurrent consumers equals num_concurrent_consumers_per_partition * num_partitions
    pub num_concurrent_consumers_per_partition: u64,
    pub num_partitions: u64,
    pub record_size_strategy: RecordSizeStrategy,
    pub record_key_allocation_strategy: RecordKeyAllocationStrategy,
    // TODO
    // pub use_smart_module: Vec<bool>,
}

impl BenchmarkSettings {
    pub fn total_number_of_messages_produced_per_batch(&self) -> u64 {
        self.num_records_per_producer_worker_per_batch as u64
            * self.num_concurrent_producer_workers as u64
    }

    pub fn number_of_expected_times_each_message_consumed(&self) -> u64 {
        self.num_concurrent_consumers_per_partition
    }
}

pub fn generate_new_topic_name() -> String {
    let mut rng = thread_rng();
    let chars: String = (0..10).map(|_| rng.sample(Alphanumeric) as char).collect();
    format!("benchmarking-{}", chars)
}
