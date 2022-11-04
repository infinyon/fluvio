use std::sync::{
    atomic::{AtomicU64, Ordering},
};

use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub(crate) struct SpuMetrics {
    pub(crate) inbound: Activity,
    pub(crate) outbound: Activity,
    smartmodule: SmartModuleChainMetrics,
}

impl SpuMetrics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn chain_metrics(&self) -> &SmartModuleChainMetrics {
        &self.smartmodule
    }
}

/*
pub(crate) struct SpuMetricsTopicPartition<'a> {
    metrics: &'a SpuMetrics,
    _topic: &'a str,
    _partition: i32,
}

impl<'a> SpuMetricsTopicPartition<'a> {
    pub(crate) fn increase(&self,records: u64,bytes: u64)
        self.metrics.records_read.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_read(&self, value: u64) {
        self.metrics.bytes_read.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn increase_inbound_records(&self, value: u64) {
        self.metrics
            .records_write
            .fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn increase_inbound_bytes(&self, value: u64) {
        self.metrics
            .bytes_written
            .fetch_add(value, Ordering::SeqCst);
    }
}
*/

#[derive(Default, Debug, Serialize)]
pub(crate) struct Record {
    records: AtomicU64,
    bytes: AtomicU64,
}

impl Record {
    pub(crate) fn increase(&self, records: u64, bytes: u64) {
        self.records.fetch_add(records, Ordering::SeqCst);
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
    }
}

#[derive(Default, Debug, Serialize)]
pub(crate) struct Activity {
    connector: Record,
    client: Record,
}

impl Activity {
    pub(crate) fn increase(&self, connector: bool, records: u64, bytes: u64) {
        if connector {
            self.connector.increase(records, bytes);
        } else {
            self.client.increase(records, bytes);
        }
    }
}
