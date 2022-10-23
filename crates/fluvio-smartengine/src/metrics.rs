use std::sync::{
    atomic::{AtomicU64, Ordering},
};

use serde::Serialize;

#[derive(Serialize, Default, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_out: AtomicU64,
    invocation_count: AtomicU64,
}

impl SmartModuleChainMetrics {
    pub fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, Ordering::SeqCst);
        self.invocation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, Ordering::SeqCst);
    }
}
