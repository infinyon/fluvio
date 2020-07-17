//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::TryInto;
use std::io::Error as IoError;

use log::debug;

use kf_protocol::bytes::Buf;

use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;

use kf_protocol::api::api_decode;
use kf_protocol::derive::Encode;

use super::versions::ApiVersionsRequest;
use super::metadata::*;
use super::objects::*;

use super::AdminPublicApiKey;

#[derive(Debug, Encode)]
pub enum AdminPublicRequest {
    // Mixed
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    CreateRequest(RequestMessage<CreateRequest>),
    DeleteRequest(RequestMessage<DeleteRequest>),
    ListRequest(RequestMessage<ListRequest>),
    WatchMetadataRequest(RequestMessage<WatchMetadataRequest>),
}

impl Default for AdminPublicRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for AdminPublicRequest {
    type ApiKey = AdminPublicApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        let api_key = header.api_key().try_into()?;
        debug!("decoding admin public request from: {} api: {:#?}", header.client_id(),api_key);
        match api_key {
            AdminPublicApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),

            AdminPublicApiKey::Create => api_decode!(Self, CreateRequest, src, header),
            AdminPublicApiKey::Delete => api_decode!(Self, DeleteRequest, src, header),
            AdminPublicApiKey::List => api_decode!(Self, ListRequest, src,header),
            AdminPublicApiKey::WatchMetadata => api_decode!(Self, WatchMetadataRequest, src, header),
        }
    }
}
