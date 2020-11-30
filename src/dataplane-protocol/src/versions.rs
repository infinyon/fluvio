use crate::ErrorCode;
use crate::api::Request;
use crate::apis::AdminPublicApiKey;
use crate::derive::{Decode, Encode};

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
    pub platform_version: String,
}

pub type ApiVersions = Vec<ApiVersionKey>;

#[derive(Decode, Encode, Default, Debug)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}
