use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::MirrorTargetApiEnum;
use super::update_offsets::UpdateTargetOffsetRequest;

/// Requests from target to source
#[derive(Debug, Encoder)]
pub enum TargetMirrorRequest {
    #[fluvio(tag = 0)]
    UpdateTargetOffset(RequestMessage<UpdateTargetOffsetRequest>),
}

impl Default for TargetMirrorRequest {
    fn default() -> Self {
        Self::UpdateTargetOffset(RequestMessage::<UpdateTargetOffsetRequest>::default())
    }
}

impl ApiMessage for TargetMirrorRequest {
    type ApiKey = MirrorTargetApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            MirrorTargetApiEnum::UpdateTargetOffset => {
                Ok(Self::UpdateTargetOffset(RequestMessage::new(
                    header,
                    UpdateTargetOffsetRequest::decode_from(src, version)?,
                )))
            }
        }
    }
}
