use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use dataplane::bytes::Buf;
use dataplane::core::Decoder;
use dataplane::derive::{Encode, Decode};

use dataplane::api::{RequestMessage, ApiMessage, RequestHeader};

use super::fetch_stream_request::FetchStreamRequest;

#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum SPUPeerApiEnum {
    FetchStream = 0,
}

impl Default for SPUPeerApiEnum {
    fn default() -> Self {
        Self::FetchStream
    }
}

#[derive(Debug, Encode)]
pub enum SpuPeerRequest {
    FetchStream(RequestMessage<FetchStreamRequest>),
}

impl Default for SpuPeerRequest {
    fn default() -> SpuPeerRequest {
        SpuPeerRequest::FetchStream(RequestMessage::<FetchStreamRequest>::default())
    }
}

impl ApiMessage for SpuPeerRequest {
    type ApiKey = SPUPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            SPUPeerApiEnum::FetchStream => Ok(SpuPeerRequest::FetchStream(RequestMessage::new(
                header,
                FetchStreamRequest::decode_from(src, version)?,
            ))),
        }
    }
}
