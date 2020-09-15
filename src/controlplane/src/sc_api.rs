use std::io::Error as IoError;
use std::convert::TryInto;

use dataplane_protocol::api::api_decode;
use dataplane_protocol::api::ApiMessage;
use dataplane_protocol::api::RequestHeader;
use dataplane_protocol::api::RequestMessage;
use dataplane_protocol::bytes::Buf;
use dataplane_protocol::derive::Encode;

use dataplane_protocol::derive::Decode;

use super::RegisterSpuRequest;
use super::UpdateLrsRequest;

/// API call from Spu to SC

#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum InternalScKey {
    RegisterSpu = 2000,
    UpdateLrs = 2001,
}

impl Default for InternalScKey {
    fn default() -> InternalScKey {
        InternalScKey::RegisterSpu
    }
}

/// Request made to Spu from Sc
#[derive(Debug, Encode)]
pub enum InternalScRequest {
    RegisterSpuRequest(RequestMessage<RegisterSpuRequest>),
    UpdateLrsRequest(RequestMessage<UpdateLrsRequest>),
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
        }
    }
}
