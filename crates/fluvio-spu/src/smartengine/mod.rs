use std::collections::BTreeMap;

use fluvio::{
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind, SmartModuleExtraParams,
};
use fluvio_controlplane_metadata::topic::Deduplication;
use fluvio_protocol::link::ErrorCode;

pub(crate) mod batch;
pub(crate) mod file_batch;
pub(crate) mod produce_batch;
pub(crate) mod context;
mod chain;

#[cfg(feature = "smartengine")]
pub(crate) use fluvio_smartengine::{
    EngineError, Lookback, SmartModuleChainBuilder, metrics::SmartModuleChainMetrics, SmartEngine,
    SmartModuleChainInstance, Version,
};

// Stub structures to support a null smartengine config
#[cfg(not(feature = "smartengine"))]
mod null_smartengine {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

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

    #[derive(Default)]
    pub struct SmartModuleChainBuilder;

    impl SmartModuleChainBuilder {
        pub fn set_store_memory_limit(&mut self, _max_memory_bytes: usize) {}
    }

    #[derive(Debug)]
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
            use fluvio_smartmodule::SMARTMODULE_TIMESTAMPS_VERSION;
            const DEFAULT_SMARTENGINE_VERSION: Version = SMARTMODULE_TIMESTAMPS_VERSION;

            #[allow(deprecated)]
            let records = input.try_into_records(DEFAULT_SMARTENGINE_VERSION)?;
            let out = SmartModuleOutput::new(records);
            Ok(out)
        }
    }

    pub type Version = i16;

    // copied from SmartEngine crate, refactor to remove this and config smartengine crate to export w/o specific engine later
    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Lookback {
        Last(u64),
        Age { age: Duration, last: u64 },
    }

    #[allow(dead_code)]
    #[derive(thiserror::Error, Debug)]
    pub enum EngineError {
        #[error("No valid smartmodule found")]
        UnknownSmartModule,
        #[error("Failed to instantiate: {0}")]
        Instantiate(anyhow::Error),
        #[error("Requested memory {requested}b exceeded max allowed {max}b")]
        StoreMemoryExceeded {
            current: usize,
            requested: usize,
            max: usize,
        },
    }
}

#[cfg(not(feature = "smartengine"))]
pub(crate) use null_smartengine::*;

pub(crate) fn dedup_to_invocation(dedup: &Deduplication) -> SmartModuleInvocation {
    use fluvio_smartmodule::dataplane::smartmodule::Lookback;

    let lookback = Lookback {
        last: dedup.bounds.count,
        age: dedup.bounds.age,
    };
    let mut params = BTreeMap::new();
    params.insert("count".to_owned(), dedup.bounds.count.to_string());
    if let Some(age) = dedup.bounds.age {
        params.insert("age".to_string(), age.as_millis().to_string());
    };
    for (k, v) in dedup.filter.transform.with.iter() {
        params.insert(k.clone(), v.clone());
    }
    SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(dedup.filter.transform.uses.clone()),
        kind: SmartModuleKind::Filter,
        params: SmartModuleExtraParams::new(params, Some(lookback)),
    }
}

pub(crate) fn map_engine_error(err: &EngineError) -> ErrorCode {
    match err {
        EngineError::UnknownSmartModule => ErrorCode::Other("Unknown SmartModule type".to_string()),
        EngineError::Instantiate(err) => ErrorCode::Other(err.to_string()),
        EngineError::StoreMemoryExceeded {
            current: _,
            requested,
            max,
        } => ErrorCode::SmartModuleMemoryLimitExceeded {
            requested: *requested as u64,
            max: *max as u64,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fluvio_controlplane_metadata::topic::{Bounds, Filter, Transform};

    use super::*;

    #[test]
    fn test_dedup_to_inv() {
        //when
        let dedup = Deduplication {
            bounds: Bounds {
                count: 1,
                age: Some(Duration::from_secs(1)),
            },
            filter: Filter {
                transform: Transform {
                    uses: "filter@0.1.0".to_string(),
                    with: BTreeMap::from([("param_name".to_string(), "param_value".to_string())]),
                },
            },
        };
        //when
        let inv = dedup_to_invocation(&dedup);

        //then
        assert!(matches!(
            inv.wasm,
            SmartModuleInvocationWasm::Predefined(str) if str.eq("filter@0.1.0")
        ));
        assert!(matches!(inv.kind, SmartModuleKind::Filter));
        assert_eq!(inv.params.get("count"), Some(&"1".to_string()));
        assert_eq!(inv.params.get("age"), Some(&"1000".to_string()));
        assert_eq!(
            inv.params.get("param_name"),
            Some(&"param_value".to_string())
        );
    }
}
