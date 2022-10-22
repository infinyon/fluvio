use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub struct ClientMetrics {
    consumer: RecordCounter,
    producer: RecordCounter,
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
}

cfg_if::cfg_if! {
    if #[cfg(target_arch = "wasm32")] {

        #[derive(Default, Debug, Serialize)]
        pub struct RecordCounter {
            pub records: u64,
            pub bytes: u64,
        }

        impl RecordCounter {
            #[inline]
            pub(crate) fn add_records(&self, value: u64) {
                self.records += value;
            }

            #[inline]
            pub(crate) fn add_bytes(&self, value: u64) {
                self.bytes += value;
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
