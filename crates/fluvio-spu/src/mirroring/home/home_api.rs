use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::MirrorHomeApiEnum;
use super::update_offsets::UpdateHomeOffsetRequest;

/// Requests from home to remote
#[derive(Debug, Encoder)]
pub enum HomeMirrorRequest {
    #[fluvio(tag = 0)]
    UpdateHomeOffset(RequestMessage<UpdateHomeOffsetRequest>),
}

impl Default for HomeMirrorRequest {
    fn default() -> Self {
        Self::UpdateHomeOffset(RequestMessage::<UpdateHomeOffsetRequest>::default())
    }
}

impl ApiMessage for HomeMirrorRequest {
    type ApiKey = MirrorHomeApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            MirrorHomeApiEnum::UpdateHomeOffset => Ok(Self::UpdateHomeOffset(RequestMessage::new(
                header,
                UpdateHomeOffsetRequest::decode_from(src, version)?,
            ))),
        }
    }
}
