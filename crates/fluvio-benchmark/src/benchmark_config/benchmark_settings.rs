use std::time::Duration;

use fluvio::Compression;
use serde::{Serialize, Deserialize};

use super::benchmark_matrix::RecordSizeStrategy;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkSettings {
    /// Each sample is a collection of batches that all run on the same topic.
    pub num_samples: usize,
    pub num_batches_per_sample: usize,
    pub duration_between_batches: Duration,
    pub num_records_per_producer_worker_per_batch: usize,
    pub producer_queue_size: usize,
    pub producer_linger: Duration,
    pub producer_server_timeout: Duration,
    pub producer_compression: Compression,
    // TODO
    // pub producer_isolation:...,
    // TODO
    // pub producer_delivery_semantic,
    pub consumer_max_bytes: usize,
    // TODO
    // pub consumer_isolation:...,
    pub num_concurrent_producer_workers: usize,
    /// Total number of concurrent consumers equals num_concurrent_consumers_per_partition * num_partitions
    pub num_concurrent_consumers_per_partition: usize,
    pub num_partitions: usize,
    pub record_size_strategy: RecordSizeStrategy,
    // TODO
    // pub use_smart_module: Vec<bool>,
}
