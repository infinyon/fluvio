use std::io::Error as IoError;
use std::convert::TryInto;

use kf_protocol::api::api_decode;
use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;
use kf_protocol::bytes::Buf;
use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

use super::UpdateSpuRequest;
use super::UpdateReplicaRequest;

/// API call from SC to SPU
#[fluvio_kf(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum InternalSpuApi {
    UpdateSpu = 1001,
    UpdateReplica = 1002,
}

impl Default for InternalSpuApi {
    fn default() -> Self {
        Self::UpdateSpu
    }
}

#[derive(Debug, Encode)]
pub enum InternalSpuRequest {
    UpdateSpuRequest(RequestMessage<UpdateSpuRequest>),
    UpdateReplicaRequest(RequestMessage<UpdateReplicaRequest>),
}

// Added to satisfy Encode/Decode traits
impl Default for InternalSpuRequest {
    fn default() -> Self {
        Self::UpdateSpuRequest(RequestMessage::default())
    }
}

impl InternalSpuRequest {
    pub fn new_update_spu_req(msg: UpdateSpuRequest) -> Self {
        Self::UpdateSpuRequest(RequestMessage::new_request(msg))
    }
}

impl KfRequestMessage for InternalSpuRequest {
    type ApiKey = InternalSpuApi;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        match header.api_key().try_into()? {
            InternalSpuApi::UpdateSpu => api_decode!(Self, UpdateSpuRequest, src, header),
            InternalSpuApi::UpdateReplica => api_decode!(Self, UpdateReplicaRequest, src, header),
        }
    }
}
