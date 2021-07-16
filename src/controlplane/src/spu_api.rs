use std::io::Error as IoError;
use std::convert::TryInto;

use dataplane::api::api_decode;
use dataplane::api::ApiMessage;
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;
use dataplane::bytes::Buf;
use fluvio_protocol::Encoder;
use fluvio_protocol::Decoder;

use super::UpdateSpuRequest;
use super::UpdateReplicaRequest;

#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum InternalSpuApi {
    UpdateSpu = 1001,
    UpdateReplica = 1002,
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
        }
    }
}
