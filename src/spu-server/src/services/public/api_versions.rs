use std::io::Error;
use log::debug;

use spu_api::SpuApiKey;
use spu_api::versions::ApiVersionKey;
use spu_api::versions::ApiVersionsRequest;
use spu_api::versions::ApiVersionsResponse;
use spu_api::spus::FlvFetchLocalSpuRequest;
use spu_api::offsets::FlvFetchOffsetsRequest;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::api::Request;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::fetch::DefaultKfFetchRequest;

pub async fn handle_kf_lookup_version_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    debug!("generating api response");

    let mut response = ApiVersionsResponse::default();

    // Kafka
    response.api_keys.push(make_version_key(
        SpuApiKey::KfProduce,
        DefaultKfProduceRequest::MIN_API_VERSION,
        DefaultKfProduceRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuApiKey::KfFetch,
        DefaultKfFetchRequest::MIN_API_VERSION,
        DefaultKfFetchRequest::MAX_API_VERSION,
    ));

    // Fluvio
    response.api_keys.push(make_version_key(
        SpuApiKey::FlvFetchLocalSpu,
        FlvFetchLocalSpuRequest::DEFAULT_API_VERSION,
        FlvFetchLocalSpuRequest::DEFAULT_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuApiKey::FlvFetchOffsets,
        FlvFetchOffsetsRequest::DEFAULT_API_VERSION,
        FlvFetchOffsetsRequest::DEFAULT_API_VERSION,
    ));

    Ok(request.new_response(response))
}

/// Build version key object
fn make_version_key(key: SpuApiKey, min_version: i16, max_version: i16) -> ApiVersionKey {
    let api_key = key as i16;
    ApiVersionKey {
        api_key,
        min_version,
        max_version,
    }
}
