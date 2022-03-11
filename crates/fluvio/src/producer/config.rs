use std::time::Duration;

use tracing::debug;

use derive_builder::Builder;
use once_cell::sync::Lazy;

use fluvio_compression::Compression;

use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};

const DEFAULT_LINGER_MS: u64 = 100;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_384;
static DEFAULT_COMPRESSION_CODEC: Lazy<Compression> = Lazy::new(|| {
    use std::env;
    let var_value =
        env::var("FLV_CLIENT_DEFAULT_COMPRESSION_CODEC").unwrap_or_else(|_| "none".to_string());
    let compression: Compression = var_value.parse().unwrap_or_else(|e| {
        debug!(?e, "unknown compression format in FLV_CLIENT_DEFAULT_COMPRESSION_CODEC, using Compression::None");
        Compression::None
    });
    compression
});

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE_BYTES
}

fn default_linger_duration() -> Duration {
    Duration::from_millis(DEFAULT_LINGER_MS)
}

fn default_partitioner() -> Box<dyn Partitioner + Send + Sync> {
    Box::new(SiphashRoundRobinPartitioner::new())
}

fn default_compression() -> Compression {
    *DEFAULT_COMPRESSION_CODEC
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
    #[builder(default = "default_compression()")]
    pub(crate) compression: Compression,
}

impl Default for TopicProducerConfig {
    fn default() -> Self {
        Self {
            linger: default_linger_duration(),
            batch_size: default_batch_size(),
            partitioner: default_partitioner(),
            compression: default_compression(),
        }
    }
}
