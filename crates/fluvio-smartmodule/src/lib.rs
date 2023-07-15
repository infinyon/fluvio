#![doc = include_str!("../README.md")]

mod input;
mod output;
mod error;

use std::ops::Deref;

pub use fluvio_smartmodule_derive::{smartmodule, SmartOpt};
pub const ENCODING_ERROR: i32 = -1;

pub use eyre::Error;
pub use eyre::eyre;

pub type Result<T> = eyre::Result<T>;

/// used only in smartmodule
#[cfg(feature = "smartmodule")]
pub mod memory;

pub use fluvio_protocol::record::{Record, RecordData};

pub use crate::input::SMARTMODULE_LTA_VERSION;

/// remap to old data plane
pub mod dataplane {
    pub mod smartmodule {
        pub use fluvio_protocol::link::smartmodule::*;

        pub use crate::input::*;
        pub use crate::output::*;
        pub use crate::error::*;
        pub use crate::SmartModuleRecord;
    }

    pub mod core {
        pub use fluvio_protocol::*;
    }

    pub mod record {
        pub use fluvio_protocol::record::*;
    }
}

/// Wrapper on `Record` that provides access to the base offset and timestamp
pub struct SmartModuleRecord {
    inner_record: Record,
    base_offset: i64,
    base_timestamp: i64,
}

impl SmartModuleRecord {
    pub fn new(inner_record: Record, base_offset: i64, base_timestamp: i64) -> Self {
        Self {
            inner_record,
            base_offset,
            base_timestamp,
        }
    }

    pub fn into_inner(self) -> Record {
        self.inner_record
    }

    pub fn timestamp(&self) -> i64 {
        self.base_timestamp + self.inner_record.timestamp_delta()
    }

    pub fn offset(&self) -> i64 {
        self.base_offset + self.inner_record.preamble.offset_delta()
    }

    pub fn key(&self) -> Option<&RecordData> {
        self.inner_record.key()
    }

    pub fn value(&self) -> &RecordData {
        self.inner_record.value()
    }
}

impl Deref for SmartModuleRecord {
    type Target = Record;

    fn deref(&self) -> &Self::Target {
        &self.inner_record
    }
}

impl From<SmartModuleRecord> for Record {
    fn from(sm_record: SmartModuleRecord) -> Self {
        sm_record.into_inner()
    }
}
