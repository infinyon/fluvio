#![allow(clippy::assign_op_pattern)]

//!
//! # API Versions
//!
//! Public API to retrieve a list of APIs and their version numbers from the SC.
//! SC supports Kafka as well as Fluvio specific APIs.
//!

pub use fluvio_protocol::link::versions::{ApiVersionKey, ApiVersions};
pub use fluvio_protocol::link::versions::{ApiVersionsRequest, ApiVersionsResponse};
use crate::apis::AdminPublicApiKey;

/// Given an API key, it returns max_version. None if not found
pub fn lookup_version(api_key: AdminPublicApiKey, versions: &[ApiVersionKey]) -> Option<i16> {
    for version in versions {
        if version.api_key == api_key as i16 {
            return Some(version.max_version);
        }
    }
    None
}
