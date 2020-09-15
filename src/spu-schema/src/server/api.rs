// ApiRequest and Response that has all request and response
// use for generic dump and client

use tracing::trace;
use std::convert::TryInto;
use std::io::Error as IoError;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Decoder;
use fluvio_protocol::derive::Encode;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::api::api_decode;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;


use dataplane_protocol::produce::DefaultProduceRequest;

use dataplane_protocol::fetch::FileFetchRequest;

use super::SpuServerApiKey;
use super::fetch_offset::FetchOffsetsRequest;
use super::versions::ApiVersionsRequest;
use super::register_replica::RegisterSyncReplicaRequest;
use super::stream_fetch::FileStreamFetchRequest;

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
            SpuServerApiKey::FetchOffsets => {
                api_decode!(Self, FetchOffsetsRequest, src, header)
            }
            SpuServerApiKey::RegisterSyncReplicaRequest => {
                api_decode!(Self, RegisterSyncReplicaRequest, src, header)
            }
            SpuServerApiKey::StreamFetch => api_decode!(Self, FileStreamFetchRequest, src, header),
        }
    }
}
