//!
//! # SC -- Query API Versions
//!
//! Communicates with SC Controller to retrieve version information
//!

use sc_api::versions::{ApiVersions, ApiVersionsRequest};
use sc_api::apis::ScApiKey;

use crate::error::CliError;
use crate::common::Connection;

/// Query for API versions
pub async fn sc_get_api_versions(conn: &mut Connection) -> Result<ApiVersions, CliError> {
    // Version is None, as we want API to request max_version.
    let response = conn
        .send_request(ApiVersionsRequest::default(), None)
        .await?;
    Ok(response.api_keys)
}

/// Given an API key, it returns max_version. None if not found
pub fn sc_lookup_version(api_key: ScApiKey, versions: &ApiVersions) -> Option<i16> {
    for version in versions {
        if version.api_key == api_key as i16 {
            return Some(version.max_version);
        }
    }
    None
}
