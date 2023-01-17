use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_in: AtomicU64,
    records_out: AtomicU64,
    smartmodule_usage: AtomicU64,
    invocation_count: AtomicU64,
}

impl SmartModuleChainMetrics {
    pub fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, Ordering::SeqCst);
        self.invocation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_records_in(&self, value: u64) {
        self.records_in.fetch_add(value, Ordering::SeqCst);
    }

    pub fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, Ordering::SeqCst);
    }

    /// value = max(total_records_in, total_records_out) where
    /// total_records_in = Number of records passed to each smartmodule in the chain.
    /// total_records_out =Number of records returned from each smartmodule in the chain.
    pub fn add_smartmodule_usage(&self, value: u64) {
        self.smartmodule_usage.fetch_add(value, Ordering::SeqCst);
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::SeqCst)
    }

    pub fn records_in(&self) -> u64 {
        self.records_in.load(Ordering::SeqCst)
    }

    pub fn records_out(&self) -> u64 {
        self.records_out.load(Ordering::SeqCst)
    }

    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(Ordering::SeqCst)
    }
    /// Aggregation of `usage = max(total_records_in, total_records_out)` where
    /// total_records_in = Number of records passed to each smartmodule in the chain.
    /// total_records_out =Number of records returned from each smartmodule in the chain.
    pub fn smartmodule_usage(&self) -> u64 {
        self.smartmodule_usage.load(Ordering::SeqCst)
    }
}
