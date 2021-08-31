#![allow(clippy::len_without_is_empty)]
#![allow(clippy::should_implement_trait)]

mod common;
mod error_code;

pub mod batch;
pub mod record;
pub mod fetch;
pub mod produce;
pub mod versions;
pub mod smartstream;

#[cfg(feature = "fixture")]
pub mod fixture;

pub use common::*;
pub use error_code::*;

pub use fluvio_protocol as core;
pub use fluvio_protocol::api;
pub use fluvio_protocol::bytes;
pub use fluvio_protocol::derive;

#[cfg(feature = "file")]
pub use fluvio_protocol::store;
