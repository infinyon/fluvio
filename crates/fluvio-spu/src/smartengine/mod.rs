pub(crate) mod batch;
pub(crate) mod file_batch;
pub(crate) mod produce_batch;
pub(crate) mod context;
mod chain;

#[cfg(feature = "smartengine")]
pub(crate) use fluvio_smartengine::{
    Lookback, metrics::SmartModuleChainMetrics, SmartEngine, SmartModuleChainInstance, Version,
};

// Stub structures to support a null smartengine config
#[cfg(not(feature = "smartengine"))]
mod null_smartengine {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};

    use anyhow::Result;
    use serde::{Deserialize, Serialize};

    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleOutput;
    use fluvio_smartmodule::Record;

    // refactor to use more widely as a "flow" metric?
    // hack copy of smartmodule chain metrics
    #[derive(Debug, Default, Deserialize, Serialize)]
    pub struct SmartModuleChainMetrics {
        bytes_in: AtomicU64,
        records_out: AtomicU64,
        invocation_count: AtomicU64,
        fuel_used: AtomicU64,
    }

    #[allow(dead_code)]
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

    #[derive(Clone, Debug, Default)]
    pub struct SmartEngine;

    impl SmartEngine {
        pub fn new() -> Self {
            SmartEngine {}
        }
    }

    pub struct SmartModuleChainInstance;

    impl SmartModuleChainInstance {
        pub async fn look_back<F, R>(
            &mut self,
            _read_fn: F,
            _metrics: &SmartModuleChainMetrics,
        ) -> Result<()>
        where
            R: Future<Output = Result<Vec<Record>>>,
            F: Fn(Lookback) -> R,
        {
            Ok(())
        }

        pub fn process(
            &mut self,
            input: SmartModuleInput,
            _metric: &SmartModuleChainMetrics,
        ) -> Result<SmartModuleOutput> {
            let out = SmartModuleOutput::new(input.try_into()?);
            Ok(out)
        }
    }

    pub type Version = i16;

    // copied from SmartEngine crate, refactor to remove this and config smartengine crate to export w/o specific engine later
    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Lookback {
        Last(u64),
    }
}

#[cfg(not(feature = "smartengine"))]
pub(crate) use null_smartengine::*;
