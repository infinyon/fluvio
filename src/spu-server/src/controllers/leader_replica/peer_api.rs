use std::io::Error as IoError;
use std::convert::TryInto;

use log::trace;

use kf_protocol::bytes::Buf;
use kf_protocol::Decoder;
use kf_protocol::derive::Encode;
use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;

use super::KfLeaderPeerApiEnum;
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

impl KfRequestMessage for LeaderPeerRequest {
    type ApiKey = KfLeaderPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            KfLeaderPeerApiEnum::UpdateOffsets => Ok(LeaderPeerRequest::UpdateOffsets(
                RequestMessage::new(header, UpdateOffsetRequest::decode_from(src, version)?),
            )),
        }
    }
}
