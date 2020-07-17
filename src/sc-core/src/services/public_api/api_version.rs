use std::io::Error;
use log::trace;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::api::Request;

use sc_api::versions::ApiVersionKey;
use sc_api::versions::{ApiVersionsRequest, ApiVersionsResponse};
use sc_api::AdminPublicApiKey;
use sc_api::objects::*;
use sc_api::metadata::*;

pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let mut response = ApiVersionsResponse::default();

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
        AdminPublicApiKey::WatchMetadata,
        WatchMetadataRequest::DEFAULT_API_VERSION,
        WatchMetadataRequest::DEFAULT_API_VERSION,
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
