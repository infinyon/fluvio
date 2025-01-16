use std::time::Duration;

use clap::{Parser, ValueEnum};
use fluvio::Compression;
use serde::{Deserialize, Serialize};
use bytesize::ByteSize;

use crate::utils;

const DEFAULT_BATCH_SIZE: ByteSize = ByteSize(16_384);
const DEFAULT_QUEUE_SIZE: u64 = 10;
const DEFAULT_MAX_REQUEST_SIZE: ByteSize = ByteSize(33_554_432);
const DEFAULT_LINGER: &str = "0ms";
const DEFAULT_SERVER_TIMEOUT: &str = "5000ms";
const DEFAULT_COMPRESSION: Compression = Compression::None;
const DEFAULT_NUM_SAMPLES: usize = 3;
const DEFAULT_TIME_BETWEEN_SAMPLES: &str = "250ms";
const DEFAULT_WORKER_TIMEOUT: &str = "3000s";
const DEFAULT_RECORD_KEY_ALLOCATION_STRATEGY: RecordKeyAllocationStrategy =
    RecordKeyAllocationStrategy::NoKey;
const DEFAULT_NUM_PRODUCERS: u64 = 1;
const DEFAULT_RECORD_SIZE: ByteSize = ByteSize(5120);
const DEFAULT_NUM_RECORDS: u64 = 10_000;
const DEFAULT_PARTITIONS: u32 = 1;
const DEFAULT_REPLICAS: u32 = 1;
const DEFAULT_DELETE_TOPIC: bool = false;

#[derive(Debug, Parser, Clone)]
pub struct ProducerConfig {
    /// Size of each batch
    #[arg(short, long, value_name = "bytes", default_value_t = DEFAULT_BATCH_SIZE)]
    pub batch_size: ByteSize,
    /// Number of records to send
    #[arg(short, long, default_value_t = DEFAULT_QUEUE_SIZE)]
    pub queue_size: u64,
    /// Maximum size of a request
    #[arg(long, value_name = "bytes", default_value_t = DEFAULT_MAX_REQUEST_SIZE)]
    pub max_request_size: ByteSize,
    /// Time to wait for new records
    #[arg(short, long, value_parser = humantime::parse_duration, default_value = DEFAULT_LINGER)]
    pub linger: Duration,
    /// Timeout for the server
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_SERVER_TIMEOUT)]
    pub server_timeout: Duration,
    /// Compression algorithm to use
    #[arg(short, long, default_value_t = DEFAULT_COMPRESSION)]
    pub compression: Compression,
    #[clap(flatten)]
    pub shared_config: SharedConfig,
}

#[derive(Debug, Parser, Clone)]
pub struct ConsumerConfig {
    #[arg(short, long, value_name = "bytes", default_value_t = DEFAULT_BATCH_SIZE)]
    pub batch_size: bytesize::ByteSize,
    /// Number of records to fetch
    #[arg(short, long, default_value_t = DEFAULT_QUEUE_SIZE)]
    pub queue_size: u64,
    /// Maximum size of a request
    #[arg(short, long, value_name = "bytes", default_value_t = DEFAULT_MAX_REQUEST_SIZE)]
    pub max_request_size: bytesize::ByteSize,
    /// Time to wait for new records
    #[arg(short, long, value_parser = humantime::parse_duration, default_value = DEFAULT_LINGER)]
    pub linger: Duration,
    /// Timeout for the server
    #[arg(short, long, value_parser = humantime::parse_duration, default_value = DEFAULT_SERVER_TIMEOUT)]
    pub server_timeout: Duration,
    #[clap(flatten)]
    pub shared_config: SharedConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, Parser)]
pub struct SharedConfig {
    /// Number of samples to take
    #[arg(long, default_value_t = DEFAULT_NUM_SAMPLES)]
    pub num_samples: usize,
    /// Time between each sample
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_TIME_BETWEEN_SAMPLES)]
    pub time_between_samples: Duration,
    /// Timeout for each worker
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_WORKER_TIMEOUT)]
    pub worker_timeout: Duration,
    #[clap(flatten)]
    pub topic_config: FluvioTopicConfig,
    #[clap(flatten)]
    pub load_config: BenchmarkLoadConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, Parser)]
pub struct BenchmarkLoadConfig {
    /// Strategy for allocating record keys
    #[clap(long, value_enum, default_value_t = DEFAULT_RECORD_KEY_ALLOCATION_STRATEGY)]
    pub record_key_allocation_strategy: RecordKeyAllocationStrategy,
    /// Number of producers that will send records
    #[clap(long, default_value_t = DEFAULT_NUM_PRODUCERS)]
    pub num_producers: u64,
    /// Number of records each producer will send
    #[clap(long, default_value_t = DEFAULT_NUM_RECORDS)]
    pub num_records: u64,
    /// Size of each record in bytes
    #[arg(long, value_name = "bytes", default_value_t = DEFAULT_RECORD_SIZE)]
    pub record_size: ByteSize,
}

#[derive(Debug, Serialize, Deserialize, Clone, Parser)]
pub struct FluvioTopicConfig {
    /// Number of partitions for the topic
    #[clap(short, long, default_value_t = DEFAULT_PARTITIONS)]
    pub partitions: u32,
    /// Number of replicas for the topic
    #[clap(short, long, default_value_t = DEFAULT_REPLICAS)]
    pub replicas: u32,
    /// Name of the topic to create
    #[clap(short, long, default_value_t = default_topic_name())]
    pub topic_name: String,
    /// Delete the topic after the benchmark
    #[clap(short, long, default_value_t = DEFAULT_DELETE_TOPIC)]
    pub delete_topic: bool,
    /// Ignore rack assignment
    #[clap(long, default_value_t = DEFAULT_DELETE_TOPIC)]
    pub ignore_rack: bool,
}

#[derive(Debug, Parser, ValueEnum, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[clap(rename_all = "kebab-case")]
pub enum RecordKeyAllocationStrategy {
    /// RecordKey::NULL
    NoKey,
    /// All producer workers will use the same key
    AllShareSameKey,
    /// Each producer will use the same key for each of their records
    ProducerWorkerUniqueKey,
    /// Each record will have a unique key
    RandomKey,
}

pub fn default_topic_name() -> String {
    format!(
        "benchmark-{}",
        utils::generate_random_string(10).to_lowercase()
    )
}
