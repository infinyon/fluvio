#![allow(clippy::assign_op_pattern)]

#[cfg(feature = "file")]
pub mod server;

pub mod client;
pub mod fetch;
pub mod produce;
mod isolation;

#[cfg(feature = "file")]
pub mod file_record;

pub mod errors {
    pub use fluvio_protocol::api::ErrorCode;
}

pub use fluvio_protocol::api::versions::{ApiVersions, ApiVersionsRequest, ApiVersionsResponse};
pub use isolation::*;
