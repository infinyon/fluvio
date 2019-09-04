//!
//! # Kafka -- Query Metadata
//!
//! Communicates with Kafka Controller to retrieve version information
//!
use kf_protocol::message::api_versions::KfApiVersionsRequest;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;

/// Query for API versions
pub async fn kf_get_api_versions(conn: &mut Connection) -> Result<KfApiVersions, CliError> {
    // Version is None, as we want API to request max_version.
    let response = conn
        .send_request(KfApiVersionsRequest::default(), None)
        .await?;
    Ok(response.api_keys)
}

/// Given an API key, it returns max_version. None if not found
pub fn kf_lookup_version(api_key: AllKfApiKey, versions: &KfApiVersions) -> Option<i16> {
    for version in versions {
        if version.index == api_key as i16 {
            return Some(version.max_version);
        }
    }
    None
}
