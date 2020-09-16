use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use dataplane_protocol::bytes::Buf;
use dataplane_protocol::core::Decoder;
use dataplane_protocol::derive::Encode;

use dataplane_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::FollowerPeerApiEnum;
use super::DefaultSyncRequest;

#[derive(Debug, Encode)]
pub enum FollowerPeerRequest {
    SyncRecords(RequestMessage<DefaultSyncRequest>),
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
        }
    }
}
