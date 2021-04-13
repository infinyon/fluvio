use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use dataplane::bytes::Buf;
use dataplane::core::Decoder;
use dataplane::derive::Encode;

use dataplane::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::FollowerPeerApiEnum;
use super::sync::DefaultSyncRequest;

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
