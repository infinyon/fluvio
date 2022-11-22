use std::time::Duration;

use fluvio::Compression;
use serde::{Serialize, Deserialize};

use super::benchmark_matrix::{RecordSizeStrategy, RecordKeyAllocationStrategy, SharedSettings};
use rand::{Rng, thread_rng, distributions::Uniform};

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
    let chars: String = (0..15)
        .map(|_| rng.sample(Uniform::new(b'a', b'z')) as char)
        .collect();
    format!("benchmarking-{}", chars)
}

#[derive(Clone)]
pub struct BenchmarkBuilder {
    pub shared_settings: SharedSettings,
    pub num_records_per_producer_worker_per_batch: Option<u64>,
    pub producer_batch_size: Option<u64>,
    pub producer_queue_size: Option<u64>,
    pub producer_linger: Option<Duration>,
    pub producer_server_timeout: Option<Duration>,
    pub producer_compression: Option<Compression>,
    // TODO
    // pub producer_isolation: Option<...>,
    // TODO
    // pub producer_delivery_semantic>,
    pub consumer_max_bytes: Option<u64>,
    // TODO
    // pub consumer_isolation: Option<...>,
    pub num_concurrent_producer_workers: Option<u64>,
    /// Total number of concurrent consumers equals num_concurrent_consumers_per_partition * num_partitions
    pub num_concurrent_consumers_per_partition: Option<u64>,
    pub num_partitions: Option<u64>,
    pub record_size_strategy: Option<RecordSizeStrategy>,
    pub record_key_allocation_strategy: Option<RecordKeyAllocationStrategy>,
    // TODO
    // pub use_smart_module: Vec<bool>,
}
impl BenchmarkBuilder {
    pub fn new(shared_settings: &SharedSettings) -> Self {
        Self {
            shared_settings: shared_settings.clone(),
            num_records_per_producer_worker_per_batch: Default::default(),
            producer_batch_size: Default::default(),
            producer_queue_size: Default::default(),
            producer_linger: Default::default(),
            producer_server_timeout: Default::default(),
            producer_compression: Default::default(),
            consumer_max_bytes: Default::default(),
            num_concurrent_producer_workers: Default::default(),
            num_concurrent_consumers_per_partition: Default::default(),
            num_partitions: Default::default(),
            record_size_strategy: Default::default(),
            record_key_allocation_strategy: Default::default(),
        }
    }
}

impl From<BenchmarkBuilder> for BenchmarkSettings {
    fn from(x: BenchmarkBuilder) -> Self {
        BenchmarkSettings {
            topic_name: generate_new_topic_name(),
            worker_timeout: Duration::from_secs(x.shared_settings.worker_timeout_seconds),
            num_samples: x.shared_settings.num_samples,
            num_batches_per_sample: x.shared_settings.num_batches_per_sample,
            duration_between_batches: Duration::from_millis(
                x.shared_settings.millis_between_batches,
            ),
            num_records_per_producer_worker_per_batch: x
                .num_records_per_producer_worker_per_batch
                .unwrap(),
            producer_batch_size: x.producer_batch_size.unwrap(),
            producer_queue_size: x.producer_queue_size.unwrap(),
            producer_linger: x.producer_linger.unwrap(),
            producer_server_timeout: x.producer_server_timeout.unwrap(),
            producer_compression: x.producer_compression.unwrap(),
            consumer_max_bytes: x.consumer_max_bytes.unwrap(),
            num_concurrent_producer_workers: x.num_concurrent_producer_workers.unwrap(),
            num_concurrent_consumers_per_partition: x
                .num_concurrent_consumers_per_partition
                .unwrap(),
            num_partitions: x.num_partitions.unwrap(),
            record_size_strategy: x.record_size_strategy.unwrap(),
            record_key_allocation_strategy: x.record_key_allocation_strategy.unwrap(),
        }
    }
}

pub trait CrossIterate {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkBuilder) + Copy>(
        self: Self,
        values: &[T],
        f: F,
    ) -> Self;
    fn build(self: Self) -> Vec<BenchmarkSettings>;
}

impl CrossIterate for Vec<BenchmarkBuilder> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkBuilder) + Copy>(
        self: Self,
        values: &[T],
        f: F,
    ) -> Self {
        self.into_iter()
            .flat_map(|builder| {
                values.iter().map(move |value| {
                    let mut clone = builder.clone();
                    f(value.clone(), &mut clone);
                    clone
                })
            })
            .collect()
    }

    fn build(self: Self) -> Vec<BenchmarkSettings> {
        self.into_iter().map(|x| x.into()).collect()
    }
}
