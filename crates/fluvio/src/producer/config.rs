use std::time::Duration;

use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};

const DEFAULT_LINGER_MS: u64 = 250;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_000;

/// Builder for `TopicProducerConfig`
#[derive(Default)]
pub struct TopicProducerConfigBuilder {
    batch_size: Option<usize>,
    linger_ms: Option<u64>,
    partitioner: Option<Box<dyn Partitioner + Send + Sync>>,
}

impl TopicProducerConfigBuilder {
    /// Sets the batch size
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the linger configuration in milliseconds
    pub fn linger_ms(mut self, linger_ms: u64) -> Self {
        self.linger_ms = Some(linger_ms);
        self
    }

    pub fn partitioner(mut self, partitioner: Box<dyn Partitioner + Send + Sync>) -> Self {
        self.partitioner = Some(partitioner);
        self
    }

    /// Creates a `TopicProducerConfig` with the current configurations.
    pub fn build(self) -> TopicProducerConfig {
        TopicProducerConfig {
            batch_size: self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE_BYTES),
            linger: Duration::from_millis(self.linger_ms.unwrap_or(DEFAULT_LINGER_MS)),
            partitioner: self
                .partitioner
                .unwrap_or_else(|| Box::new(SiphashRoundRobinPartitioner::new())),
        }
    }
}
/// Options used to adjust the behavior of the Producer.
/// Create this struct with `TopicProducerConfigBuilder`.
///
/// // Use config on this call:
/// fluvio.topic_producer_with_config("topic", config).await?;
/// # Ok(())
/// # }
/// ```
pub struct TopicProducerConfig {
    pub(crate) batch_size: usize,
    pub(crate) linger: Duration,
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
