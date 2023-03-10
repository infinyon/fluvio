use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::FollowerPeerApiEnum;
use super::sync::DefaultSyncRequest;
use super::reject_request::RejectOffsetRequest;

#[derive(Debug, Encoder)]
pub enum FollowerPeerRequest {
    #[fluvio(tag = 0)]
    SyncRecords(RequestMessage<DefaultSyncRequest>),
    #[fluvio(tag = 1)]
    RejectedOffsetRequest(RequestMessage<RejectOffsetRequest>),
}

impl Default for FollowerPeerRequest {
    fn default() -> FollowerPeerRequest {
        FollowerPeerRequest::SyncRecords(RequestMessage::<DefaultSyncRequest>::default())
    }
}

impl ApiMessage for FollowerPeerRequest {
    type ApiKey = FollowerPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            FollowerPeerApiEnum::SyncRecords => Ok(FollowerPeerRequest::SyncRecords(
                RequestMessage::new(header, DefaultSyncRequest::decode_from(src, version)?),
            )),
            FollowerPeerApiEnum::RejectedOffsetRequest => {
                Ok(FollowerPeerRequest::RejectedOffsetRequest(
                    RequestMessage::new(header, RejectOffsetRequest::decode_from(src, version)?),
                ))
            }
        }
    }
}
