#![allow(clippy::assign_op_pattern)]

//!
//! # API Versions
//!
//! Public API to retrive a list of APIs and their version numbers from the SC.
//! SC supports Kafka as well as Fluvio specific APIs.
//!

use dataplane::api::Request;
use dataplane::derive::{Decode, Encode};
use dataplane::ErrorCode;

use crate::AdminPublicApiKey;

pub type ApiVersions = Vec<ApiVersionKey>;

/// Given an API key, it returns max_version. None if not found
pub fn lookup_version(api_key: AdminPublicApiKey, versions: &[ApiVersionKey]) -> Option<i16> {
    for version in versions {
        if version.api_key == api_key as i16 {
            return Some(version.max_version);
        }
    }
    None
}

// -----------------------------------
// ApiVersionsRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsRequest {}

impl Request for ApiVersionsRequest {
    const API_KEY: u16 = AdminPublicApiKey::ApiVersion as u16;
    type Response = ApiVersionsResponse;
}

// -----------------------------------
// ApiVersionsResponse
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsResponse {
    pub error_code: ErrorCode,
    pub api_keys: Vec<ApiVersionKey>,
}

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}
