// ApiRequest and Response that has all request and response
// use for generic dump and client

use tracing::trace;
use std::convert::TryInto;
use std::io::Error as IoError;

use dataplane_protocol::bytes::Buf;
use dataplane_protocol::derive::Encode;

use dataplane_protocol::api::RequestMessage;

use dataplane_protocol::api::api_decode;
use dataplane_protocol::api::RequestHeader;
use dataplane_protocol::api::ApiMessage;

use super::SpuClientApiKey;
use super::offset::ReplicaOffsetUpdateRequest;

/// Request from Spu Server to Client
#[derive(Debug, Encode)]
pub enum SpuClientRequest {
    ReplicaOffsetUpdateRequest(RequestMessage<ReplicaOffsetUpdateRequest>),
}

impl Default for SpuClientRequest {
    fn default() -> Self {
        Self::ReplicaOffsetUpdateRequest(RequestMessage::<ReplicaOffsetUpdateRequest>::default())
    }
}

impl ApiMessage for SpuClientRequest {
    type ApiKey = SpuClientApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        match header.api_key().try_into()? {
            SpuClientApiKey::ReplicaOffsetUpdate => {
                api_decode!(Self, ReplicaOffsetUpdateRequest, src, header)
            }
        }
    }
}
