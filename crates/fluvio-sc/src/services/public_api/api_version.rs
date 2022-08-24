use std::io::Error;
use tracing::{trace, instrument};

use fluvio_protocol::api::{RequestMessage, ResponseMessage, Request};
use fluvio_protocol::link::versions::{
    ApiVersionKey, ApiVersionsRequest, ApiVersionsResponse, PlatformVersion,
};
use fluvio_sc_schema::objects::{
    ObjectApiCreateRequest, ObjectApiDeleteRequest, ObjectApiListRequest, ObjectApiWatchRequest,
};
use fluvio_sc_schema::AdminPublicApiKey;

#[instrument(skip(request))]
pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let mut response = ApiVersionsResponse::default();

    let platform_version = semver::Version::parse(crate::VERSION)
        .expect("Platform Version (from VERSION file) must be semver");
    response.platform_version = PlatformVersion::from(platform_version);

    // topic versions
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Create,
        ObjectApiCreateRequest::DEFAULT_API_VERSION,
        ObjectApiCreateRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Delete,
        ObjectApiDeleteRequest::DEFAULT_API_VERSION,
        ObjectApiDeleteRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::List,
        ObjectApiListRequest::DEFAULT_API_VERSION,
        ObjectApiListRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        AdminPublicApiKey::Watch,
        ObjectApiWatchRequest::DEFAULT_API_VERSION,
        ObjectApiWatchRequest::DEFAULT_API_VERSION,
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
