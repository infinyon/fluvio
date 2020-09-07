// ApiRequest and Response that has all request and response
// use for generic dump and client

use tracing::trace;
use std::convert::TryInto;
use std::io::Error as IoError;

use kf_protocol::bytes::Buf;
use kf_protocol::Decoder;
use kf_protocol::derive::Encode;

use kf_protocol::api::KfRequestMessage;

use kf_protocol::api::api_decode;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;
use kf_protocol::fs::KfFileFetchRequest;

use super::SpuServerApiKey;
use super::fetch_offset::FlvFetchOffsetsRequest;
use super::versions::ApiVersionsRequest;
use super::register_replica::RegisterSyncReplicaRequest;
use super::stream_fetch::FileStreamFetchRequest;

/// Request to Spu Server
#[derive(Debug, Encode)]
pub enum SpuServerRequest {
    /// list of versions supported
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    // Kafka compatible requests
    KfProduceRequest(RequestMessage<DefaultKfProduceRequest>),
    KfFileFetchRequest(RequestMessage<KfFileFetchRequest>),
    FlvFetchOffsetsRequest(RequestMessage<FlvFetchOffsetsRequest>),
    FileStreamFetchRequest(RequestMessage<FileStreamFetchRequest>),
    RegisterSyncReplicaRequest(RequestMessage<RegisterSyncReplicaRequest>),
}

impl Default for SpuServerRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for SpuServerRequest {
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

            // Kafka
            SpuServerApiKey::KfProduce => {
                let request = DefaultKfProduceRequest::decode_from(src, header.api_version())?;
                Ok(Self::KfProduceRequest(RequestMessage::new(header, request)))
            }
            SpuServerApiKey::KfFetch => api_decode!(Self, KfFileFetchRequest, src, header),
            SpuServerApiKey::FlvFetchOffsets => {
                api_decode!(Self, FlvFetchOffsetsRequest, src, header)
            }
            SpuServerApiKey::RegisterSyncReplicaRequest => {
                api_decode!(Self, RegisterSyncReplicaRequest, src, header)
            },
            SpuServerApiKey::StreamFetch => api_decode!(Self, FileStreamFetchRequest, src, header)
        }
    }
}
