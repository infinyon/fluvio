use std::{
    time::{Duration, SystemTime},
    hash::Hash,
};
use derive_builder::Builder;
use chrono::{DateTime, Utc};
use rand::{Rng, thread_rng, distributions::Uniform};
use serde::{Serialize, Deserialize};
use fluvio::{Compression, Isolation, DeliverySemantic};

use self::benchmark_matrix::{RecordKeyAllocationStrategy, SharedConfig};

pub mod benchmark_matrix;
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]

pub struct Millis(u64);
impl Millis {
    pub fn new(millis: u64) -> Self {
        Self(millis)
    }
}

impl From<Millis> for Duration {
    fn from(m: Millis) -> Self {
        Duration::from_millis(m.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Seconds(u64);
impl Seconds {
    pub fn new(seconds: u64) -> Self {
        Self(seconds)
    }
}

impl From<Seconds> for Duration {
    fn from(m: Seconds) -> Self {
        Duration::from_secs(m.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
pub struct BenchmarkConfig {
    pub matrix_name: String,
    #[builder(private)]
    pub topic_name: String,
    pub current_profile: String,
    #[builder(private)]
    pub timestamp: DateTime<Utc>,
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
    pub producer_isolation: Isolation,
    pub producer_delivery_semantic: DeliverySemantic,
    pub consumer_max_bytes: u64,
    pub consumer_isolation: Isolation,
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
        // We don't compare topic_name, current_profile, or timestamp
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
            && self.producer_delivery_semantic == other.producer_delivery_semantic
            && self.producer_isolation == other.producer_isolation
            && self.consumer_max_bytes == other.consumer_max_bytes
            && self.consumer_isolation == other.consumer_isolation
            && self.num_concurrent_producer_workers == other.num_concurrent_producer_workers
            && self.num_concurrent_consumers_per_partition
                == other.num_concurrent_consumers_per_partition
            && self.num_partitions == other.num_partitions
            && self.record_size == other.record_size
            && self.record_key_allocation_strategy == other.record_key_allocation_strategy
            && self.matrix_name == other.matrix_name
    }
}

impl Hash for BenchmarkConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // We don't hash the topic name, current_profile, or timestamp
        self.worker_timeout.hash(state);
        self.num_samples.hash(state);
        self.duration_between_samples.hash(state);
        self.num_records_per_producer_worker_per_batch.hash(state);
        self.producer_batch_size.hash(state);
        self.producer_queue_size.hash(state);
        self.producer_linger.hash(state);
        self.producer_server_timeout.hash(state);
        self.producer_compression.hash(state);
        self.producer_isolation.hash(state);
        self.producer_delivery_semantic.hash(state);
        self.consumer_max_bytes.hash(state);
        self.consumer_isolation.hash(state);
        self.num_concurrent_producer_workers.hash(state);
        self.num_concurrent_consumers_per_partition.hash(state);
        self.num_partitions.hash(state);
        self.record_size.hash(state);
        self.record_key_allocation_strategy.hash(state);
        self.matrix_name.hash(state);
    }
}

impl BenchmarkConfig {
    pub fn total_number_of_messages_produced_per_batch(&self) -> u64 {
        self.num_records_per_producer_worker_per_batch * self.num_concurrent_producer_workers
    }

    pub fn number_of_expected_times_each_message_consumed(&self) -> u64 {
        self.num_concurrent_consumers_per_partition
    }

    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("**Config**\n");
        md.push_str("```yaml");
        md.push_str(&serde_yaml::to_string(self).unwrap());
        md.push_str("```");
        md
    }
}

pub fn generate_new_topic_name() -> String {
    let mut rng = thread_rng();
    let chars: String = (0..15)
        .map(|_| rng.sample(Uniform::new(b'a', b'z')) as char)
        .collect();
    format!("benchmarking-{chars}")
}
impl BenchmarkConfigBuilder {
    fn prebuild(&mut self) -> &mut Self {
        self.topic_name(generate_new_topic_name());
        self.timestamp(SystemTime::now().into());
        self
    }
    pub fn new(shared_config: &SharedConfig, profile: String) -> Self {
        let mut s = Self::default();
        s.matrix_name(shared_config.matrix_name.clone())
            .num_samples(shared_config.num_samples)
            .duration_between_samples(shared_config.millis_between_samples.into())
            .worker_timeout(shared_config.worker_timeout_seconds.into())
            .current_profile(profile);
        s
    }
}

pub trait CrossIterate {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkConfigBuilder) + Copy>(
        self,
        values: &[T],
        f: F,
    ) -> Self;
    fn build(self) -> Vec<BenchmarkConfig>;
}

impl CrossIterate for Vec<BenchmarkConfigBuilder> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut BenchmarkConfigBuilder) + Copy>(
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
        self.into_iter()
            .map(|mut x| x.prebuild().build().unwrap())
            .collect()
    }
}
