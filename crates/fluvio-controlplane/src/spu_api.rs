use std::io::Error as IoError;
use std::convert::TryInto;

use dataplane::api::api_decode;
use dataplane::api::ApiMessage;
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;
use dataplane::bytes::Buf;
use dataplane::derive::Encoder;
use dataplane::derive::Decoder;

use super::UpdateSpuRequest;
use super::UpdateReplicaRequest;
use super::UpdateSmartModuleRequest;
use super::UpdateSmartStreamRequest;

#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum InternalSpuApi {
    UpdateSpu = 1001,
    UpdateReplica = 1002,
    UpdateSmartModule = 1003,
    UpdateSmartStream = 1004,
}

impl Default for InternalSpuApi {
    fn default() -> Self {
        Self::UpdateSpu
    }
}

#[derive(Debug, Encoder)]
pub enum InternalSpuRequest {
    UpdateSpuRequest(RequestMessage<UpdateSpuRequest>),
    UpdateReplicaRequest(RequestMessage<UpdateReplicaRequest>),
    UpdateSmartModuleRequest(RequestMessage<UpdateSmartModuleRequest>),
    UpdateSmartStreamRequest(RequestMessage<UpdateSmartStreamRequest>),
}

// Added to satisfy Encoder/Decoder traits
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
            InternalSpuApi::UpdateSmartModule => {
                api_decode!(Self, UpdateSmartModuleRequest, src, header)
            }
            InternalSpuApi::UpdateSmartStream => {
                api_decode!(Self, UpdateSmartStreamRequest, src, header)
            }
        }
    }
}
