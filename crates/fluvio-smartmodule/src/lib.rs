#![doc = include_str!("../README.md")]

pub use fluvio_protocol::record::{Record, RecordData};
pub use fluvio_protocol as dataplane;

pub use fluvio_smartmodule_derive::{smartmodule, SmartOpt};

pub const ENCODING_ERROR: i32 = -1;

pub use eyre::Error;
pub type Result<T> = eyre::Result<T>;

mod error;
pub use error::*;

/// used only in smartmodule
#[cfg(feature = "smartmodule")]
pub mod memory;

mod input;
pub use input::*;

mod output;
pub use output::*;
