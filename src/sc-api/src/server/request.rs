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

use super::versions::ApiVersionsRequest;
use super::spu::*;
use super::topic::*;
use super::update_all_metadata::*;

use super::ScServerApiKey;

#[derive(Debug, Encode)]
pub enum ScServerRequest {
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

    UpdateAllMetadataRequest(RequestMessage<UpdateAllMetadataRequest>),
}

impl Default for ScServerRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for ScServerRequest {
    type ApiKey = ScServerApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding header: {:#?}", header);
        match header.api_key().try_into()? {
            // Mixed
            ScServerApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),

            //Kafka
            ScServerApiKey::KfMetadata => api_decode!(Self, KfMetadataRequest, src, header),

            // Fluvio - Topics
            ScServerApiKey::FlvCreateTopics => {
                api_decode!(Self, FlvCreateTopicsRequest, src, header)
            }
            ScServerApiKey::FlvDeleteTopics => {
                api_decode!(Self, FlvDeleteTopicsRequest, src, header)
            }
            ScServerApiKey::FlvFetchTopics => api_decode!(Self, FlvFetchTopicsRequest, src, header),
            ScServerApiKey::FlvTopicComposition => {
                api_decode!(Self, FlvTopicCompositionRequest, src, header)
            }

            // Fluvio - Custom Spus / Spu Groups
            ScServerApiKey::FlvRegisterCustomSpus => {
                api_decode!(Self, FlvRegisterCustomSpusRequest, src, header)
            }
            ScServerApiKey::FlvUnregisterCustomSpus => {
                api_decode!(Self, FlvUnregisterCustomSpusRequest, src, header)
            }
            ScServerApiKey::FlvFetchSpus => api_decode!(Self, FlvFetchSpusRequest, src, header),

            ScServerApiKey::FlvCreateSpuGroups => {
                api_decode!(Self, FlvCreateSpuGroupsRequest, src, header)
            }
            ScServerApiKey::FlvDeleteSpuGroups => {
                api_decode!(Self, FlvDeleteSpuGroupsRequest, src, header)
            }
            ScServerApiKey::FlvFetchSpuGroups => {
                api_decode!(Self, FlvFetchSpuGroupsRequest, src, header)
            }

            ScServerApiKey::FlvUpdateAllMetadata => {
                api_decode!(Self, UpdateAllMetadataRequest, src, header)
            }
        }
    }
}
