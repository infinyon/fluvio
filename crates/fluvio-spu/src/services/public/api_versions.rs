use std::io::Error;
use tracing::{trace, instrument};

use fluvio_protocol::api::{RequestMessage, ResponseMessage, Request};
use fluvio_spu_schema::produce::DefaultProduceRequest;
use fluvio_spu_schema::fetch::DefaultFetchRequest;
use fluvio_protocol::link::versions::ApiVersionKey;
use fluvio_spu_schema::server::SpuServerApiKey;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::stream_fetch::DefaultStreamFetchRequest;
use fluvio_spu_schema::server::update_offset::UpdateOffsetsRequest;
use fluvio_spu_schema::{ApiVersionsRequest, ApiVersionsResponse};

#[instrument(skip(request))]
pub async fn handle_api_version_request(
    request: RequestMessage<ApiVersionsRequest>,
) -> Result<ResponseMessage<ApiVersionsResponse>, Error> {
    let client_version = &request.request.client_version;
    let mut response = ApiVersionsResponse::default();
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
    response.api_keys.push(make_version_key(
        SpuServerApiKey::StreamFetch,
        0,
        // Client version is empty in Fluvio <= 0.9.12
        if client_version.is_empty() {
            15
        } else {
            DefaultStreamFetchRequest::DEFAULT_API_VERSION
        },
    ));
    response.api_keys.push(make_version_key(
        SpuServerApiKey::UpdateOffsets,
        0,
        UpdateOffsetsRequest::DEFAULT_API_VERSION,
    ));

    trace!("Returning ApiVersionsResponse: {:#?}", &response);
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
