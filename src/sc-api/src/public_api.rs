//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::TryInto;
use std::io::Error as IoError;

use log::trace;

use kf_protocol::bytes::Buf;

use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;

use kf_protocol::api::api_decode;
use kf_protocol::derive::Encode;

use kf_protocol::message::metadata::KfMetadataRequest;

use crate::versions::ApiVersionsRequest;
use crate::spu::FlvRegisterCustomSpusRequest;
use crate::spu::FlvUnregisterCustomSpusRequest;
use crate::spu::FlvFetchSpusRequest;
use crate::spu::FlvCreateSpuGroupsRequest;
use crate::spu::FlvFetchSpuGroupsRequest;
use crate::spu::FlvDeleteSpuGroupsRequest;
use crate::topic::FlvCreateTopicsRequest;
use crate::topic::FlvDeleteTopicsRequest;
use crate::topic::FlvFetchTopicsRequest;
use crate::topic::FlvTopicCompositionRequest;

use super::ScApiKey;

#[derive(Debug, Encode)]
pub enum PublicRequest {
    // Mixed
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    // Kafka
    KfMetadataRequest(RequestMessage<KfMetadataRequest>),

    // Fluvio - Topics
    FlvCreateTopicsRequest(RequestMessage<FlvCreateTopicsRequest>),
    FlvDeleteTopicsRequest(RequestMessage<FlvDeleteTopicsRequest>),
    FlvFetchTopicsRequest(RequestMessage<FlvFetchTopicsRequest>),
    FlvTopicCompositionRequest(RequestMessage<FlvTopicCompositionRequest>),

    // Fluvio - Spus
    FlvRegisterCustomSpusRequest(RequestMessage<FlvRegisterCustomSpusRequest>),
    FlvUnregisterCustomSpusRequest(RequestMessage<FlvUnregisterCustomSpusRequest>),
    FlvFetchSpusRequest(RequestMessage<FlvFetchSpusRequest>),

    FlvCreateSpuGroupsRequest(RequestMessage<FlvCreateSpuGroupsRequest>),
    FlvDeleteSpuGroupsRequest(RequestMessage<FlvDeleteSpuGroupsRequest>),
    FlvFetchSpuGroupsRequest(RequestMessage<FlvFetchSpuGroupsRequest>),
}

impl Default for PublicRequest {
    fn default() -> PublicRequest {
        PublicRequest::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for PublicRequest {
    type ApiKey = ScApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding header: {:#?}", header);
        match header.api_key().try_into()? {
            // Mixed
            ScApiKey::ApiVersion => api_decode!(PublicRequest, ApiVersionsRequest, src, header),

            //Kafka
            ScApiKey::KfMetadata => api_decode!(PublicRequest, KfMetadataRequest, src, header),

            // Fluvio - Topics
            ScApiKey::FlvCreateTopics => {
                api_decode!(PublicRequest, FlvCreateTopicsRequest, src, header)
            }
            ScApiKey::FlvDeleteTopics => {
                api_decode!(PublicRequest, FlvDeleteTopicsRequest, src, header)
            }
            ScApiKey::FlvFetchTopics => {
                api_decode!(PublicRequest, FlvFetchTopicsRequest, src, header)
            }
            ScApiKey::FlvTopicComposition => {
                api_decode!(PublicRequest, FlvTopicCompositionRequest, src, header)
            }

            // Fluvio - Custom Spus / Spu Groups
            ScApiKey::FlvRegisterCustomSpus => {
                api_decode!(PublicRequest, FlvRegisterCustomSpusRequest, src, header)
            }
            ScApiKey::FlvUnregisterCustomSpus => {
                api_decode!(PublicRequest, FlvUnregisterCustomSpusRequest, src, header)
            }
            ScApiKey::FlvFetchSpus => api_decode!(PublicRequest, FlvFetchSpusRequest, src, header),

            ScApiKey::FlvCreateSpuGroups => {
                api_decode!(PublicRequest, FlvCreateSpuGroupsRequest, src, header)
            }
            ScApiKey::FlvDeleteSpuGroups => {
                api_decode!(PublicRequest, FlvDeleteSpuGroupsRequest, src, header)
            }
            ScApiKey::FlvFetchSpuGroups => {
                api_decode!(PublicRequest, FlvFetchSpuGroupsRequest, src, header)
            }
        }
    }
}
