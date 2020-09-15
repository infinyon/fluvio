use std::io::Error as IoError;
use std::convert::TryInto;

use dataplane_protocol::api::api_decode;
use dataplane_protocol::api::ApiMessage;
use dataplane_protocol::api::RequestHeader;
use dataplane_protocol::api::RequestMessage;
use dataplane_protocol::bytes::Buf;
use dataplane_protocol::derive::Encode;
use dataplane_protocol::derive::Decode;

use super::UpdateSpuRequest;
use super::UpdateReplicaRequest;


#[fluvio(encode_discriminant)]
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

impl ApiMessage for InternalSpuRequest {
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
