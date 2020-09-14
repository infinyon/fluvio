use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use kf_protocol::bytes::Buf;
use kf_protocol::Decoder;
use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;

use super::fetch_stream_request::FetchStreamRequest;

#[fluvio_kf(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum KfSPUPeerApiEnum {
    FetchStream = 0,
}

impl Default for KfSPUPeerApiEnum {
    fn default() -> KfSPUPeerApiEnum {
        KfSPUPeerApiEnum::FetchStream
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

impl KfRequestMessage for SpuPeerRequest {
    type ApiKey = KfSPUPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            KfSPUPeerApiEnum::FetchStream => Ok(SpuPeerRequest::FetchStream(RequestMessage::new(
                header,
                FetchStreamRequest::decode_from(src, version)?,
            ))),
        }
    }
}
