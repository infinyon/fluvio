#![doc = include_str!("../README.md")]

pub use fluvio_smartmodule_derive::{smartmodule, SmartOpt};
pub const ENCODING_ERROR: i32 = -1;

pub use eyre::Error;
pub use eyre::eyre;
pub type Result<T> = eyre::Result<T>;

/// used only in smartmodule
#[cfg(feature = "smartmodule")]
pub mod memory;

pub use fluvio_protocol::record::{Record, RecordData};
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
