use std::io::Error as IoError;
use std::convert::TryInto;

use fluvio_protocol::api::api_decode;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Encoder;
use fluvio_protocol::Decoder;

use super::UpdateSpuRequest;
use super::UpdateReplicaRequest;
use super::UpdateSmartModuleRequest;

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum InternalSpuApi {
    UpdateSpu = 1001,
    UpdateReplica = 1002,
    UpdateSmartModule = 1003,
    // UpdateDerivedStream = 1004,
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
        }
    }
}
