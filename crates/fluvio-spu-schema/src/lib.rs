#![allow(clippy::assign_op_pattern)]

pub mod server;
pub mod client;
pub mod fetch;
pub mod produce;
mod isolation;

#[cfg(feature = "file")]
pub mod file;

pub mod errors {
    pub use fluvio_protocol::link::ErrorCode;
}

pub use fluvio_protocol::link::versions::{ApiVersions, ApiVersionsRequest, ApiVersionsResponse};
pub use isolation::*;

/// Default API version for all API
pub const COMMON_VERSION: i16 = 23;
