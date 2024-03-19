use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::fetch_consumer_request::FetchConsumerRequest;
use super::fetch_stream_request::FetchStreamRequest;
use super::update_consumer_request::UpdateConsumerRequest;

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SPUPeerApiEnum {
    FetchStream = 0,
    FetchConsumer = 1,
    UpdateConsumer = 2,
}

impl Default for SPUPeerApiEnum {
    fn default() -> Self {
        Self::FetchStream
    }
}

#[derive(Debug, Encoder)]
pub enum SpuPeerRequest {
    #[fluvio(tag = 0)]
    FetchStream(RequestMessage<FetchStreamRequest>),
    #[fluvio(tag = 1)]
    FetchConsumer(RequestMessage<FetchConsumerRequest>),
    #[fluvio(tag = 2)]
    UpdateConsumer(RequestMessage<UpdateConsumerRequest>),
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
            SPUPeerApiEnum::FetchConsumer => Ok(SpuPeerRequest::FetchConsumer(
                RequestMessage::new(header, FetchConsumerRequest::decode_from(src, version)?),
            )),
            SPUPeerApiEnum::UpdateConsumer => Ok(SpuPeerRequest::UpdateConsumer(
                RequestMessage::new(header, UpdateConsumerRequest::decode_from(src, version)?),
            )),
        }
    }
}
