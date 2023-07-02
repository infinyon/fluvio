#![doc = include_str!("../README.md")]

use std::ops::Deref;

pub use fluvio_smartmodule_derive::{smartmodule, SmartOpt};
pub const ENCODING_ERROR: i32 = -1;

pub use eyre::Error;
pub use eyre::eyre;
pub type Result<T> = eyre::Result<T>;

/// used only in smartmodule
#[cfg(feature = "smartmodule")]
pub mod memory;

pub use fluvio_protocol::record::{RecordData, Record as FluvioRecord};

#[derive(Debug, Clone)]
pub struct Record {
    inner_record: FluvioRecord,
    base_offset: i64,
    base_timestamp: i64,
}

impl Record {
    pub fn new(inner_record: FluvioRecord, base_offset: i64, base_timestamp: i64) -> Self {
        Self {
            inner_record,
            base_offset,
            base_timestamp,
        }
    }

    pub fn into_inner(self) -> FluvioRecord {
        self.inner_record
    }

    pub fn timestamp(&self) -> i64 {
        self.base_timestamp + self.inner_record.timestamp_delta()
    }

    pub fn offset(&self) -> i64 {
        self.base_offset + self.inner_record.preamble.offset_delta()
    }
}

impl Deref for Record {
    type Target = FluvioRecord;
    fn deref(&self) -> &Self::Target {
        &self.inner_record
    }
}

/// remap to old data plane
pub mod dataplane {
    pub mod smartmodule {
        pub use crate::input::*;
        pub use crate::output::*;
        pub use crate::error::*;
        pub use fluvio_protocol::link::smartmodule::*;
    }
    pub mod core {
        pub use fluvio_protocol::*;
    }
    pub mod record {
        pub use fluvio_protocol::record::*;
    }
}

mod input;
mod output;
mod error;
