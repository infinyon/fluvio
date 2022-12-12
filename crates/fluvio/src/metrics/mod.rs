use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Serialize, Deserialize};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct ClientMetrics {
    consumer: RecordCounter,
    producer_connector: RecordCounter,
    producer_client: RecordCounter,
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

    /// producer counter from connector
    #[inline]
    pub fn producer_connector(&self) -> &RecordCounter {
        &self.producer_connector
    }

    /// producer counter from non connector producer
    #[inline]
    pub fn producer_client(&self) -> &RecordCounter {
        &self.producer_client
    }

    #[cfg(feature = "smartengine")]
    pub(crate) fn chain_metrics(&self) -> &fluvio_smartengine::metrics::SmartModuleChainMetrics {
        &self.smartmodule
    }
}

cfg_if::cfg_if! {
    if #[cfg(any(target_arch = "wasm32", target_arch = "arm"))] {

        #[derive(Default, Debug, Deserialize, Serialize)]
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


        #[derive(Default, Debug, Serialize, Deserialize)]
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
