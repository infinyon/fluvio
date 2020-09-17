use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use dataplane::bytes::Buf;
use dataplane::core::Decoder;
use dataplane::derive::Encode;
use dataplane::api::{RequestMessage, ApiMessage, RequestHeader};

use super::LeaderPeerApiEnum;
use super::UpdateOffsetRequest;

#[derive(Debug, Encode)]
pub enum LeaderPeerRequest {
    UpdateOffsets(RequestMessage<UpdateOffsetRequest>),
}

impl Default for LeaderPeerRequest {
    fn default() -> LeaderPeerRequest {
        LeaderPeerRequest::UpdateOffsets(RequestMessage::<UpdateOffsetRequest>::default())
    }
}

impl ApiMessage for LeaderPeerRequest {
    type ApiKey = LeaderPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            LeaderPeerApiEnum::UpdateOffsets => Ok(LeaderPeerRequest::UpdateOffsets(
                RequestMessage::new(header, UpdateOffsetRequest::decode_from(src, version)?),
            )),
        }
    }
}
