//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::{TryInto};
use std::io::Error as IoError;

use tracing::debug;

use dataplane::bytes::Buf;
use dataplane::api::{ApiMessage};
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;

use dataplane::api::api_decode;
use dataplane::core::Encoder;
use dataplane::versions::ApiVersionsRequest;

use crate::AdminPublicApiKey;
use crate::AdminSpec;
use crate::objects::{CreateRequest, DeleteRequest, ListRequest, WatchRequest};

#[derive(Debug, Encoder)]
pub enum AdminPublicRequest<S>
where
    S: AdminSpec,
{
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),
    CreateRequest(RequestMessage<CreateRequest<S>>),
    DeleteRequest(RequestMessage<DeleteRequest>),
    ListRequest(RequestMessage<ListRequest<S>>),
    WatchRequest(RequestMessage<WatchRequest<S>>),
}

impl<S> Default for AdminPublicRequest<S>
where
    S: AdminSpec,
{
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl<S> ApiMessage for AdminPublicRequest<S>
where
    S: AdminSpec,
{
    type ApiKey = AdminPublicApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        let api_key = header.api_key().try_into()?;
        debug!(
            "decoding admin public request from: {} api: {:#?}",
            header.client_id(),
            api_key
        );
        match api_key {
            AdminPublicApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),

            AdminPublicApiKey::Create => api_decode!(Self, CreateRequest, src, header),
            AdminPublicApiKey::Delete => api_decode!(Self, DeleteRequest, src, header),
            AdminPublicApiKey::List => api_decode!(Self, ListRequest, src, header),
            AdminPublicApiKey::Watch => api_decode!(Self, WatchRequest, src, header),
        }
    }
}
