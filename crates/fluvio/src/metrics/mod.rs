use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub struct ClientMetrics {
    consumer: RecordCounter,
    producer: RecordCounter,
    #[cfg(feature = "smartengine")]
    smartmodule: fluvio_smartengine::metrics::SmartModuleChainMetrics,
}

impl ClientMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn consumer(&self) -> &RecordCounter {
        &self.consumer
    }

    #[inline]
    pub fn producer(&self) -> &RecordCounter {
        &self.producer
    }

    #[cfg(feature = "smartengine")]
    pub(crate) fn chain_metrics(&self) -> &fluvio_smartengine::metrics::SmartModuleChainMetrics {
        &self.smartmodule
    }
}

cfg_if::cfg_if! {
    if #[cfg(any(target_arch = "wasm32", target_arch = "arm"))] {

        #[derive(Default, Debug, Serialize)]
        pub struct RecordCounter {

        }

        impl RecordCounter {
            #[inline]
            pub(crate) fn add_records(&self, _value: u64) {
            }

            #[inline]
            pub(crate) fn add_bytes(&self, _value: u64) {
            }
        }

    } else {


        #[derive(Default, Debug, Serialize)]
        pub struct RecordCounter {
            pub records: AtomicU64,
            pub bytes: AtomicU64,
        }

        impl RecordCounter {
            #[inline]
            pub(crate) fn add_records(&self, value: u64) {
                self.records.fetch_add(value, Ordering::SeqCst);
            }

            #[inline]
            pub(crate) fn add_bytes(&self, value: u64) {
                self.bytes.fetch_add(value, Ordering::SeqCst);
            }
        }

    }
}
