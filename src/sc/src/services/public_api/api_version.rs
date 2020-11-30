use std::io::Error;
use tracing::trace;

use dataplane::api::{RequestMessage, ResponseMessage, Request};

use fluvio_sc_schema::versions::ApiVersionKey;
use fluvio_sc_schema::versions::{ApiVersionsRequest, ApiVersionsResponse};
use fluvio_sc_schema::AdminPublicApiKey;
use fluvio_sc_schema::objects::*;

pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let mut response = ApiVersionsResponse::default();
    response.platform_version = crate::VERSION.to_string();

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
