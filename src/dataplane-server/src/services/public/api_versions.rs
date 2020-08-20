use std::io::Error;
use tracing::debug;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::api::Request;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::fetch::DefaultKfFetchRequest;
use spu_api::server::SpuServerApiKey;
use spu_api::server::versions::ApiVersionKey;
use spu_api::server::versions::ApiVersionsRequest;
use spu_api::server::versions::ApiVersionsResponse;
use spu_api::server::fetch_offset::FlvFetchOffsetsRequest;

pub async fn handle_kf_lookup_version_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    debug!("generating api response");

    let mut response = ApiVersionsResponse::default();

    // Kafka
    response.api_keys.push(make_version_key(
        SpuServerApiKey::KfProduce,
        DefaultKfProduceRequest::MIN_API_VERSION,
        DefaultKfProduceRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuServerApiKey::KfFetch,
        DefaultKfFetchRequest::MIN_API_VERSION,
        DefaultKfFetchRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuServerApiKey::FlvFetchOffsets,
        FlvFetchOffsetsRequest::DEFAULT_API_VERSION,
        FlvFetchOffsetsRequest::DEFAULT_API_VERSION,
    ));

    Ok(request.new_response(response))
}

/// Build version key object
fn make_version_key(key: SpuServerApiKey, min_version: i16, max_version: i16) -> ApiVersionKey {
    let api_key = key as i16;
    ApiVersionKey {
        api_key,
        min_version,
        max_version,
    }
}
