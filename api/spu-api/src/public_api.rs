// ApiRequest and Response that has all request and response
// use for generic dump and client

use log::trace;
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
use kf_socket::KfFileFetchRequest;

use crate::SpuApiKey;
use crate::spus::FlvFetchLocalSpuRequest;
use crate::offsets::FlvFetchOffsetsRequest;
use crate::versions::ApiVersionsRequest;

#[derive(Debug, Encode)]
pub enum PublicRequest {
    // Mixed
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    // Kafka
    KfProduceRequest(RequestMessage<DefaultKfProduceRequest>),
    KfFileFetchRequest(RequestMessage<KfFileFetchRequest>),

    // Fluvio
    FlvFetchLocalSpuRequest(RequestMessage<FlvFetchLocalSpuRequest>),
    FlvFetchOffsetsRequest(RequestMessage<FlvFetchOffsetsRequest>),
}

impl Default for PublicRequest {
    fn default() -> PublicRequest {
        PublicRequest::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for PublicRequest {
    type ApiKey = SpuApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        match header.api_key().try_into()? {
            // Mixed
            SpuApiKey::ApiVersion => api_decode!(PublicRequest, ApiVersionsRequest, src, header),

            // Kafka
            SpuApiKey::KfProduce => {
                let request = DefaultKfProduceRequest::decode_from(src, header.api_version())?;
                Ok(PublicRequest::KfProduceRequest(RequestMessage::new(
                    header, request,
                )))
            }
            SpuApiKey::KfFetch => api_decode!(PublicRequest, KfFileFetchRequest, src, header),

            // Fluvio
            SpuApiKey::FlvFetchLocalSpu => {
                api_decode!(PublicRequest, FlvFetchLocalSpuRequest, src, header)
            }
            SpuApiKey::FlvFetchOffsets => {
                api_decode!(PublicRequest, FlvFetchOffsetsRequest, src, header)
            }
        }
    }
}
