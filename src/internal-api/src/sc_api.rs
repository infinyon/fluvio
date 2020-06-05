use std::io::Error as IoError;
use std::convert::TryInto;

use kf_protocol::api::api_decode;
use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::RequestMessage;
use kf_protocol::bytes::Buf;
use kf_protocol::derive::Encode;

use kf_protocol::derive::Decode;

use super::RegisterSpuRequest;
use super::UpdateLrsRequest;

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

impl KfRequestMessage for InternalScRequest {
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
