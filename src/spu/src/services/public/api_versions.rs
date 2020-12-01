use std::io::Error;
use tracing::debug;

use dataplane::api::{RequestMessage, ResponseMessage, Request};
use dataplane::produce::DefaultProduceRequest;
use dataplane::fetch::DefaultFetchRequest;
use dataplane::versions::ApiVersionKey;
use fluvio_spu_schema::server::SpuServerApiKey;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::{ApiVersionsRequest, ApiVersionsResponse};

pub async fn handle_kf_lookup_version_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    debug!("generating api response");

    let mut response = ApiVersionsResponse::default();

    // Kafka
    response.api_keys.push(make_version_key(
        SpuServerApiKey::Produce,
        DefaultProduceRequest::MIN_API_VERSION,
        DefaultProduceRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuServerApiKey::Fetch,
        DefaultFetchRequest::MIN_API_VERSION,
        DefaultFetchRequest::MAX_API_VERSION,
    ));
    response.api_keys.push(make_version_key(
        SpuServerApiKey::FetchOffsets,
        FetchOffsetsRequest::DEFAULT_API_VERSION,
        FetchOffsetsRequest::DEFAULT_API_VERSION,
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
