//!
//! # API Versions
//!
//! Public API to retrive a list of APIs and their version numbers from the SPU.
//! SPU supports Kafka as well as Fluvio specific APIs.
//!

use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};
use kf_protocol::api::FlvErrorCode;

use super::SpuServerApiKey;

pub type ApiVersions = Vec<ApiVersionKey>;

// -----------------------------------
// ApiVersionsRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsRequest {}

// -----------------------------------
// ApiVersionsResponse
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionsResponse {
    pub error_code: FlvErrorCode,
    pub api_keys: Vec<ApiVersionKey>,
}

#[derive(Decode, Encode, Default, Clone, Debug)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

// -----------------------------------
// Implementation - ApiVersionsRequest
// -----------------------------------

impl Request for ApiVersionsRequest {
    const API_KEY: u16 = SpuServerApiKey::ApiVersion as u16;
    type Response = ApiVersionsResponse;
}
