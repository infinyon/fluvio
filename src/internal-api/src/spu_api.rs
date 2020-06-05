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
use super::UpdateAllRequest;

#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum InternalSpuApi {
    UpdateAll = 1000,
    UpdateSpu = 1001,
    UpdateReplica = 1003,
}

impl Default for InternalSpuApi {
    fn default() -> InternalSpuApi {
        InternalSpuApi::UpdateSpu
    }
}

#[derive(Debug, Encode)]
pub enum InternalSpuRequest {
    UpdateAllRequest(RequestMessage<UpdateAllRequest>),
    UpdateSpuRequest(RequestMessage<UpdateSpuRequest>),
    UpdateReplicaRequest(RequestMessage<UpdateReplicaRequest>),
}

// Added to satisfy Encode/Decode traits
impl Default for InternalSpuRequest {
    fn default() -> InternalSpuRequest {
        InternalSpuRequest::UpdateSpuRequest(RequestMessage::default())
    }
}

impl InternalSpuRequest {
    pub fn new_update_spu_req(msg: UpdateSpuRequest) -> InternalSpuRequest {
        InternalSpuRequest::UpdateSpuRequest(RequestMessage::new_request(msg))
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
            InternalSpuApi::UpdateAll => {
                api_decode!(InternalSpuRequest, UpdateAllRequest, src, header)
            }
            InternalSpuApi::UpdateSpu => {
                api_decode!(InternalSpuRequest, UpdateSpuRequest, src, header)
            }
            InternalSpuApi::UpdateReplica => {
                api_decode!(InternalSpuRequest, UpdateReplicaRequest, src, header)
            }
        }
    }
}
