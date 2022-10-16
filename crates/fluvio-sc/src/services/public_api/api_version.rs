use tracing::{trace, instrument, debug};
use semver::Version;
use once_cell::sync::Lazy;
use anyhow::Result;

use fluvio_protocol::api::{RequestMessage, ResponseMessage, Request};
use fluvio_protocol::link::versions::{
    ApiVersionKey, ApiVersionsRequest, ApiVersionsResponse, PlatformVersion,
};
use fluvio_sc_schema::objects::{
    ObjectApiCreateRequest, ObjectApiDeleteRequest, ObjectApiListRequest, ObjectApiWatchRequest,
};
use fluvio_sc_schema::AdminPublicApiKey;

// Fluvi Client version 0.14.0 corresponds to Platform version 10.0.0

static PLATFORM_VER: Lazy<Version> = Lazy::new(|| Version::parse(crate::VERSION).unwrap());

#[instrument(skip(request))]
pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>> {
    let mut response = ApiVersionsResponse {
        platform_version: PlatformVersion::new(&PLATFORM_VER),
        ..Default::default()
    };

    let client_version = Version::parse(&request.request().client_version)?;
    debug!(client_version = %client_version, "client version");

    // topic versions
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Create,
        ObjectApiCreateRequest::MIN_API_VERSION,
        ObjectApiCreateRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Delete,
        ObjectApiDeleteRequest::MIN_API_VERSION,
        ObjectApiDeleteRequest::MAX_API_VERSION,
    ));

    response.api_keys.push(make_version_key(
        AdminPublicApiKey::List,
        ObjectApiListRequest::MIN_API_VERSION,
        ObjectApiListRequest::MAX_API_VERSION,
    ));

    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Watch,
        ObjectApiWatchRequest::MIN_API_VERSION,
        ObjectApiWatchRequest::MAX_API_VERSION,
    ));

    trace!("flv api versions response: {:#?}", response);

    Ok(request.new_response(response))
}

/// Build version key object
fn make_version_key(key: AdminPublicApiKey, min_version: i16, max_version: i16) -> ApiVersionKey {
    let api_key = key as i16;
    ApiVersionKey {
        api_key,
        min_version,
        max_version,
    }
}
