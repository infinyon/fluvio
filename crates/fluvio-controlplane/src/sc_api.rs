use std::io::Error as IoError;
use std::convert::TryInto;

use dataplane::api::api_decode;
use dataplane::api::ApiMessage;
use dataplane::api::RequestHeader;
use dataplane::api::RequestMessage;
use dataplane::bytes::Buf;
use dataplane::derive::Encoder;

use dataplane::derive::Decoder;

use super::RegisterSpuRequest;
use super::UpdateLrsRequest;
use super::ReplicaRemovedRequest;

/// API call from Spu to SC

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum InternalScKey {
    RegisterSpu = 2000,
    UpdateLrs = 2001,
    ReplicaRemoved = 2002,
}

impl Default for InternalScKey {
    fn default() -> InternalScKey {
        InternalScKey::RegisterSpu
    }
}

/// Request made to Spu from Sc
#[derive(Debug, Encoder)]
pub enum InternalScRequest {
    RegisterSpuRequest(RequestMessage<RegisterSpuRequest>),
    UpdateLrsRequest(RequestMessage<UpdateLrsRequest>),
    ReplicaRemovedRequest(RequestMessage<ReplicaRemovedRequest>),
}

impl Default for InternalScRequest {
    fn default() -> InternalScRequest {
        InternalScRequest::RegisterSpuRequest(RequestMessage::default())
    }
}

impl ApiMessage for InternalScRequest {
    type ApiKey = InternalScKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        match header.api_key().try_into()? {
            InternalScKey::RegisterSpu => {
                api_decode!(InternalScRequest, RegisterSpuRequest, src, header)
            }
            InternalScKey::UpdateLrs => {
                api_decode!(InternalScRequest, UpdateLrsRequest, src, header)
            }
            InternalScKey::ReplicaRemoved => {
                api_decode!(InternalScRequest, ReplicaRemovedRequest, src, header)
            }
        }
    }
}
