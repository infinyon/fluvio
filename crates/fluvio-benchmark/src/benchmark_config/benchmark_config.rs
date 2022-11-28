use std::{time::Duration, hash::Hash, fmt::Display};
use rand::{Rng, thread_rng, distributions::Uniform};
use serde::{Serialize, Deserialize};
use fluvio::Compression;
use super::benchmark_matrix::{RecordKeyAllocationStrategy, SharedConfig};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkConfig {
    pub topic_name: String,
    /// Each sample is a collection of batches that all run on the same topic.
    pub worker_timeout: Duration,
    pub num_samples: usize,
    pub duration_between_samples: Duration,
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
    pub record_size: u64,
    pub record_key_allocation_strategy: RecordKeyAllocationStrategy,
    // TODO
    // pub use_smart_module: Vec<bool>,
}

impl Eq for BenchmarkConfig {}

impl PartialEq for BenchmarkConfig {
    fn eq(&self, other: &Self) -> bool {
        // We don't compare topic_name
        self.worker_timeout == other.worker_timeout
            && self.num_samples == other.num_samples
            && self.duration_between_samples == other.duration_between_samples
            && self.num_records_per_producer_worker_per_batch
                == other.num_records_per_producer_worker_per_batch
            && self.producer_batch_size == other.producer_batch_size
            && self.producer_queue_size == other.producer_queue_size
            && self.producer_linger == other.producer_linger
            && self.producer_server_timeout == other.producer_server_timeout
            && self.producer_compression == other.producer_compression
            && self.consumer_max_bytes == other.consumer_max_bytes
            && self.num_concurrent_producer_workers == other.num_concurrent_producer_workers
            && self.num_concurrent_consumers_per_partition
                == other.num_concurrent_consumers_per_partition
            && self.num_partitions == other.num_partitions
            && self.record_size == other.record_size
            && self.record_key_allocation_strategy == other.record_key_allocation_strategy
    }
}

impl Hash for BenchmarkConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // We don't hash the topic name
        self.worker_timeout.hash(state);
        self.num_samples.hash(state);
        self.duration_between_samples.hash(state);
        self.num_records_per_producer_worker_per_batch.hash(state);
        self.producer_batch_size.hash(state);
        self.producer_queue_size.hash(state);
        self.producer_linger.hash(state);
        self.producer_server_timeout.hash(state);
        self.producer_compression.hash(state);
        self.consumer_max_bytes.hash(state);
        self.num_concurrent_producer_workers.hash(state);
        self.num_concurrent_consumers_per_partition.hash(state);
        self.num_partitions.hash(state);
        self.record_size.hash(state);
        self.record_key_allocation_strategy.hash(state);
    }
}

impl BenchmarkConfig {
    pub fn total_number_of_messages_produced_per_batch(&self) -> u64 {
        self.num_records_per_producer_worker_per_batch as u64
            * self.num_concurrent_producer_workers as u64
    }

    pub fn number_of_expected_times_each_message_consumed(&self) -> u64 {
        self.num_concurrent_consumers_per_partition
    }
}

impl Display for BenchmarkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BenchmarkConfig:")?;
        writeln!(
            f,
            "  Number of Samples: {} (Duration between samples of {:?})",
            self.num_samples, self.duration_between_samples
        )?;
        let produced_mb = (self.num_records_per_producer_worker_per_batch
            * self.record_size
            * self.num_concurrent_producer_workers) as f64
            / 1000000.0;
        writeln!(
            f,
            "  Total produced size of sample: {} records * {} bytes per record  * {} producers = {:.3}mb",
            self.num_records_per_producer_worker_per_batch,
            self.record_size,
            self.num_concurrent_producer_workers,
           produced_mb,
        )?;
        writeln!(f,
        "  Producer details: linger {:?}, fluvio batch size {}, queue size {}, server timeout {:?}, compression {:?}",
            self.producer_linger,
            self.producer_batch_size,
            self.producer_queue_size,
            self.producer_server_timeout,
            self.producer_compression)?;
        writeln!(
            f,
            "  Total consumed size of sample: {} consumers * {:.3}mb =  {:.3}mb",
            self.num_concurrent_consumers_per_partition,
            produced_mb,
            produced_mb * self.num_concurrent_consumers_per_partition as f64
        )?;

        writeln!(
            f,
            "  Consumer details: Max bytes: {}",
            self.consumer_max_bytes
        )?;
        if self.record_key_allocation_strategy != RecordKeyAllocationStrategy::NoKey {
            writeln!(
                f,
                "  Key allocation strategy: {:?}",
                self.record_key_allocation_strategy
            )?;
        }
        Ok(())
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
    pub shared_config: SharedConfig,
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
    pub record_size_strategy: Option<u64>,
    pub record_key_allocation_strategy: Option<RecordKeyAllocationStrategy>,
    // TODO
    // pub use_smart_module: Vec<bool>,
}
impl BenchmarkBuilder {
    pub fn new(shared_config: &SharedConfig) -> Self {
        Self {
            shared_config: shared_config.clone(),
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

impl From<BenchmarkBuilder> for BenchmarkConfig {
    fn from(x: BenchmarkBuilder) -> Self {
        BenchmarkConfig {
            topic_name: generate_new_topic_name(),
            worker_timeout: x.shared_config.worker_timeout_seconds.into(),
            num_samples: x.shared_config.num_samples,
            duration_between_samples: x.shared_config.millis_between_samples.into(),
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
            record_size: x.record_size_strategy.unwrap(),
            record_key_allocation_strategy: x.record_key_allocation_strategy.unwrap(),
        }
    }
}

pub trait CrossIterate {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkBuilder) + Copy>(
        self,
        values: &[T],
        f: F,
    ) -> Self;
    fn build(self) -> Vec<BenchmarkConfig>;
}

impl CrossIterate for Vec<BenchmarkBuilder> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkBuilder) + Copy>(
        self,
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

    fn build(self) -> Vec<BenchmarkConfig> {
        self.into_iter().map(|x| x.into()).collect()
    }
}
