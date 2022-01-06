use std::time::Duration;

use derive_builder::Builder;

use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};

const DEFAULT_LINGER_MS: u64 = 100;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_384;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE_BYTES
}

fn default_linger_duration() -> Duration {
    Duration::from_millis(DEFAULT_LINGER_MS)
}

fn default_partitioner() -> Box<dyn Partitioner + Send + Sync> {
    Box::new(SiphashRoundRobinPartitioner::new())
}
/// Options used to adjust the behavior of the Producer.
/// Create this struct with [`TopicProducerConfigBuilder`].
///
/// Create a producer with a custom config with [`Fluvio::topic_producer_with_config()`].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct TopicProducerConfig {
    /// Maximum amount of bytes accumulated by the records before sending the batch.
    #[builder(default = "default_batch_size()")]
    pub(crate) batch_size: usize,
    /// Time to wait before sending messages to the server.
    #[builder(default = "default_linger_duration()")]
    pub(crate) linger: Duration,
    /// Partitioner assigns the partition to each record that needs to be send
    #[builder(default = "default_partitioner()")]
    pub(crate) partitioner: Box<dyn Partitioner + Send + Sync>,
}

impl Default for TopicProducerConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(DEFAULT_LINGER_MS),
            batch_size: DEFAULT_BATCH_SIZE_BYTES,
            partitioner: Box::new(SiphashRoundRobinPartitioner::new()),
        }
    }
}
