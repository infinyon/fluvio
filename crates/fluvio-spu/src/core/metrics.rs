use std::sync::{
    atomic::{AtomicU64, Ordering},
};

use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub(crate) struct SpuMetrics {
    records_read: AtomicU64,
    records_write: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    smartmodule: SmartModuleChainMetrics,
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
            _topic: topic,
            _partition: partition,
        }
    }

    pub(crate) fn chain_metrics(&self) -> &SmartModuleChainMetrics {
        &self.smartmodule
    }
}

pub(crate) struct SpuMetricsTopicPartition<'a> {
    metrics: &'a SpuMetrics,
    _topic: &'a str,
    _partition: i32,
}

impl<'a> SpuMetricsTopicPartition<'a> {
    pub(crate) fn add_records_read(&self, value: u64) {
        self.metrics.records_read.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_read(&self, value: u64) {
        self.metrics.bytes_read.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_records_written(&self, value: u64) {
        self.metrics
            .records_write
            .fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_written(&self, value: u64) {
        self.metrics
            .bytes_written
            .fetch_add(value, Ordering::SeqCst);
    }
}

#[derive(Serialize, Default, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_out: AtomicU64,
}

impl SmartModuleChainMetrics {
    pub(crate) fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, Ordering::SeqCst);
    }
}
