use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Default)]
pub struct SmartModuleChainMetrics {
    bytes_in: Arc<AtomicU64>,
    records_out: Arc<AtomicU64>,
}

impl SmartModuleChainMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, Ordering::SeqCst);
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Default)]
pub struct SmartModuleMetrics {
    invocations: Arc<AtomicU64>,
    records: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl SmartModuleMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn inc_invocations(&self) {
        self.invocations.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn add_records_out(&self, value: u64) {
        self.records.fetch_add(value, Ordering::SeqCst);
    }

    pub(crate) fn add_bytes_in(&self, value: u64) {
        self.bytes.fetch_add(value, Ordering::SeqCst);
    }

    pub fn invocations(&self) -> u64 {
        self.invocations.load(Ordering::SeqCst)
    }

    pub fn records_out(&self) -> u64 {
        self.records.load(Ordering::SeqCst)
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }
}
