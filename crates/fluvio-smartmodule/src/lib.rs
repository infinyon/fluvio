#![doc = include_str!("../README.md")]

mod input;
mod output;
mod error;

use std::ops::{Deref, DerefMut};

use fluvio_protocol::types::Timestamp;

pub use fluvio_smartmodule_derive::{smartmodule, SmartOpt};

pub const ENCODING_ERROR: i32 = -1;

pub use eyre::Error;
pub use eyre::eyre;

pub type Result<T> = eyre::Result<T>;

/// used only in smartmodule
#[cfg(feature = "smartmodule")]
pub mod memory;

pub use fluvio_protocol::record::{Offset, Record, RecordData};

pub use crate::input::SMARTMODULE_TIMESTAMPS_VERSION;

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
#[derive(Debug, Default, Clone)]
pub struct SmartModuleRecord {
    inner_record: Record,
    base_offset: Offset,
    base_timestamp: Timestamp,
}

impl SmartModuleRecord {
    pub fn new(inner_record: Record, base_offset: Offset, base_timestamp: Timestamp) -> Self {
        Self {
            inner_record,
            base_offset,
            base_timestamp,
        }
    }

    pub fn into_inner(self) -> Record {
        self.inner_record
    }

    pub fn timestamp(&self) -> Timestamp {
        self.base_timestamp + self.inner_record.timestamp_delta()
    }

    pub fn offset(&self) -> Offset {
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

impl DerefMut for SmartModuleRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner_record
    }
}

impl From<SmartModuleRecord> for Record {
    fn from(sm_record: SmartModuleRecord) -> Self {
        sm_record.into_inner()
    }
}
