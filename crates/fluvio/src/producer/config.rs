use std::time::Duration;

use derive_builder::Builder;
use dataplane::Isolation;

use fluvio_compression::Compression;

use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};
use crate::stats::ClientStatsDataCollect;

const DEFAULT_LINGER_MS: u64 = 100;
const DEFAULT_TIMEOUT_MS: u64 = 1500;
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

fn default_timeout() -> Duration {
    Duration::from_millis(DEFAULT_TIMEOUT_MS)
}

fn default_isolation() -> Isolation {
    Isolation::default()
}

fn default_stats_collect() -> ClientStatsDataCollect {
    ClientStatsDataCollect::None
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

    /// Compression algorithm used by Fluvio producer to compress data.
    /// If there is a topic level compression and it is not compatible with this setting, the producer
    /// initialization will fail.
    #[builder(setter(into, strip_option), default)]
    pub(crate) compression: Option<Compression>,

    /// Max time duration that the server is allowed to process the batch.
    #[builder(default = "default_timeout()")]
    pub(crate) timeout: Duration,

    /// [`Isolation`] level that the producer must respect.
    /// [`Isolation::ReadCommitted`] waits for messages to be committed (replicated) before
    /// sending the response to the caller.
    /// [`Isolation::ReadUncommitted`] just waits for the leader to accept the message.
    #[builder(default = "default_isolation()")]
    pub(crate) isolation: Isolation,

    /// Collect resource and data transfer stats used by Fluvio producer
    #[builder(default = "default_stats_collect()")]
    pub(crate) stats_collect: ClientStatsDataCollect,
}

impl Default for TopicProducerConfig {
    fn default() -> Self {
        Self {
            linger: default_linger_duration(),
            batch_size: default_batch_size(),
            partitioner: default_partitioner(),
            compression: None,
            timeout: default_timeout(),
            isolation: default_isolation(),
            stats_collect: default_stats_collect(),
        }
    }
}
