// ApiRequest and Response that has all request and response
// use for generic dump and client

use tracing::trace;
use std::convert::TryInto;
use std::io::Error as IoError;

use dataplane::bytes::Buf;
use dataplane::core::Decoder;
use dataplane::derive::Encode;
use dataplane::api::ApiMessage;
use dataplane::api::api_decode;
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;

use dataplane::produce::DefaultProduceRequest;

use dataplane::fetch::FileFetchRequest;

use crate::ApiVersionsRequest;
use super::SpuServerApiKey;
use super::fetch_offset::FetchOffsetsRequest;
use super::register_replica::RegisterSyncReplicaRequest;
use super::stream_fetch::FileStreamFetchRequest;
use super::update_offset::UpdateOffsetsRequest;

/// Request to Spu Server
#[derive(Debug, Encode)]
pub enum SpuServerRequest {
    /// list of versions supported
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    // Kafka compatible requests
    ProduceRequest(RequestMessage<DefaultProduceRequest>),
    FileFetchRequest(RequestMessage<FileFetchRequest>),
    FetchOffsetsRequest(RequestMessage<FetchOffsetsRequest>),
    FileStreamFetchRequest(RequestMessage<FileStreamFetchRequest>),
    RegisterSyncReplicaRequest(RequestMessage<RegisterSyncReplicaRequest>),
    UpdateOffsetsRequest(RequestMessage<UpdateOffsetsRequest>),
}

impl Default for SpuServerRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl ApiMessage for SpuServerRequest {
    type ApiKey = SpuServerApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        match header.api_key().try_into()? {
            // Mixed
            SpuServerApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),

            SpuServerApiKey::Produce => {
                let request = DefaultProduceRequest::decode_from(src, header.api_version())?;
                Ok(Self::ProduceRequest(RequestMessage::new(header, request)))
            }
            SpuServerApiKey::Fetch => api_decode!(Self, FileFetchRequest, src, header),
            SpuServerApiKey::FetchOffsets => api_decode!(Self, FetchOffsetsRequest, src, header),
            SpuServerApiKey::RegisterSyncReplicaRequest => {
                api_decode!(Self, RegisterSyncReplicaRequest, src, header)
            }
            SpuServerApiKey::StreamFetch => api_decode!(Self, FileStreamFetchRequest, src, header),
            SpuServerApiKey::UpdateOffsets => api_decode!(Self, UpdateOffsetsRequest, src, header),
        }
    }
}
