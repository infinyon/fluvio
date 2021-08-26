#![allow(clippy::assign_op_pattern)]

pub mod server;
pub mod client;
pub mod errors {
    pub use dataplane::ErrorCode;
}

pub use dataplane::versions::{ApiVersions, ApiVersionsRequest, ApiVersionsResponse};
