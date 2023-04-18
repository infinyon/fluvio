use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_out: AtomicU64,
    invocation_count: AtomicU64,
    fuel_used: AtomicU64,
}

impl SmartModuleChainMetrics {
    pub fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, Ordering::SeqCst);
        self.invocation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, Ordering::SeqCst);
    }

    pub fn add_fuel_used(&self, value: u64) {
        self.fuel_used.fetch_add(value, Ordering::SeqCst);
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::SeqCst)
    }

    pub fn records_out(&self) -> u64 {
        self.records_out.load(Ordering::SeqCst)
    }

    pub fn fuel_used(&self) -> u64 {
        self.fuel_used.load(Ordering::SeqCst)
    }
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(Ordering::SeqCst)
    }
}
