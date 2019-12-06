//!
//! # SPU -- Query API Versions
//!
//! Communicates with Streaming Processing Unit to retrieve version information
//!

use spu_api::versions::{ApiVersions, ApiVersionsRequest};
use spu_api::SpuApiKey;

use crate::error::CliError;
use crate::common::Connection;

/// Query for API versions
pub async fn spu_get_api_versions(conn: &mut Connection) -> Result<ApiVersions, CliError> {
    // Version is None, as we want API to request max_version.
    let response = conn
        .send_request(ApiVersionsRequest::default(), None)
        .await?;
    Ok(response.api_keys)
}

/// Given an API key, it returns max_version. None if not found
pub fn spu_lookup_version(api_key: SpuApiKey, versions: &ApiVersions) -> Option<i16> {
    for version in versions {
        if version.api_key == api_key as i16 {
            return Some(version.max_version);
        }
    }
    None
}
