use std::io::Error;
use tracing::trace;

use dataplane::api::{RequestMessage, ResponseMessage, Request};
use dataplane::versions::{ApiVersionKey, ApiVersionsRequest, ApiVersionsResponse, PlatformVersion};
use fluvio_sc_schema::objects::*;
use fluvio_sc_schema::AdminPublicApiKey;

pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let mut response = ApiVersionsResponse::default();

    let platform_version = semver::Version::parse(&*crate::VERSION)
        .expect("Platform Version (from VERSION file) must be semver");
    response.platform_version = PlatformVersion::from(platform_version);

    // topic versions
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Create,
        CreateRequest::DEFAULT_API_VERSION,
        CreateRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Delete,
        DeleteRequest::DEFAULT_API_VERSION,
        DeleteRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::List,
        ListRequest::DEFAULT_API_VERSION,
        ListRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Watch,
        WatchRequest::DEFAULT_API_VERSION,
        WatchRequest::DEFAULT_API_VERSION,
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
