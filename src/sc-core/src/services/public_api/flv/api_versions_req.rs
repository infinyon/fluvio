use std::io::Error;
use log::trace;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::api::Request;

use sc_api::versions::ApiVersionKey;
use sc_api::versions::{ApiVersionsRequest, ApiVersionsResponse};
use sc_api::ScApiKey;
use sc_api::topic::FlvCreateTopicsRequest;
use sc_api::topic::FlvDeleteTopicsRequest;
use sc_api::topic::FlvFetchTopicsRequest;
use sc_api::topic::FlvTopicCompositionRequest;
use sc_api::spu::FlvFetchSpusRequest;

pub async fn handle_api_versions_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let mut response = ApiVersionsResponse::default();

    // topic versions
    response.api_keys.push(make_version_key(
        ScApiKey::FlvCreateTopics,
        FlvCreateTopicsRequest::DEFAULT_API_VERSION,
        FlvCreateTopicsRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        ScApiKey::FlvDeleteTopics,
        FlvDeleteTopicsRequest::DEFAULT_API_VERSION,
        FlvDeleteTopicsRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        ScApiKey::FlvFetchTopics,
        FlvFetchTopicsRequest::DEFAULT_API_VERSION,
        FlvFetchTopicsRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        ScApiKey::FlvTopicComposition,
        FlvTopicCompositionRequest::DEFAULT_API_VERSION,
        FlvTopicCompositionRequest::DEFAULT_API_VERSION,
    ));

    // spus versions
    response.api_keys.push(make_version_key(
        ScApiKey::FlvFetchSpus,
        FlvFetchSpusRequest::DEFAULT_API_VERSION,
        FlvFetchSpusRequest::DEFAULT_API_VERSION,
    ));

    trace!("flv api versions response: {:#?}",response);

    Ok(request.new_response(response))
}

/// Build version key object
fn make_version_key(key: ScApiKey, min_version: i16, max_version: i16) -> ApiVersionKey {
    let api_key = key as i16;
    ApiVersionKey {
        api_key,
        min_version,
        max_version,
    }
}
