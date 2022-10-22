use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};

#[derive(Clone, Default)]
pub(crate) struct SpuMetrics {
    records_read: Arc<AtomicI64>,
    records_write: Arc<AtomicI64>,
    bytes_read: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
}

impl SpuMetrics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_topic_partition<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
    ) -> SpuMetricsTopicPartition {
        SpuMetricsTopicPartition {
            metrics: self,
            topic,
            partition,
        }
    }

    pub(crate) fn records_read(&self) -> u64 {
        self.records_read.load(Ordering::SeqCst) as u64
    }

    pub(crate) fn records_write(&self) -> u64 {
        self.records_write.load(Ordering::SeqCst) as u64
    }

    pub(crate) fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::SeqCst)
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for SpuMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpuMetrics").finish()
    }
}

pub(crate) struct SpuMetricsTopicPartition<'a> {
    metrics: &'a SpuMetrics,
    topic: &'a str,
    partition: i32,
}

impl<'a> SpuMetricsTopicPartition<'a> {
    pub(crate) fn add_records_read(&self, value: u64) {
        self.metrics
            .records_read
            .fetch_add(value as i64, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_read(&self, value: u64) {
        self.metrics.bytes_read.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_records_written(&self, value: u64) {
        self.metrics
            .records_write
            .fetch_add(value as i64, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_written(&self, value: u64) {
        self.metrics
            .bytes_written
            .fetch_add(value, Ordering::SeqCst);
    }
}
