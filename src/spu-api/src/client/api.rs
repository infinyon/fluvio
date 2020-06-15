// ApiRequest and Response that has all request and response
// use for generic dump and client

use log::trace;
use std::convert::TryInto;
use std::io::Error as IoError;

use kf_protocol::bytes::Buf;
use kf_protocol::derive::Encode;

use kf_protocol::api::KfRequestMessage;

use kf_protocol::api::api_decode;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;

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

impl KfRequestMessage for SpuClientRequest {
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
