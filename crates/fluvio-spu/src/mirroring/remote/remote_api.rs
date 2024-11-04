use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Decoder;
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::MirrorRemoteApiEnum;
use super::sync::DefaultRemotePartitionSyncRequest;
use super::update_offsets::UpdateRemoteOffsetRequest;

/// Requests from remote to home
#[derive(Debug)]
pub enum RemoteMirrorRequest {
    SyncRecords(RequestMessage<DefaultRemotePartitionSyncRequest>),
    UpdateRemoteOffset(RequestMessage<UpdateRemoteOffsetRequest>),
}

impl Default for RemoteMirrorRequest {
    fn default() -> Self {
        Self::SyncRecords(RequestMessage::<DefaultRemotePartitionSyncRequest>::default())
    }
}

impl ApiMessage for RemoteMirrorRequest {
    type ApiKey = MirrorRemoteApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            MirrorRemoteApiEnum::UpdateEdgeOffset => {
                Ok(Self::UpdateRemoteOffset(RequestMessage::new(
                    header,
                    UpdateRemoteOffsetRequest::decode_from(src, version)?,
                )))
            }
            MirrorRemoteApiEnum::SyncRecords => Ok(Self::SyncRecords(RequestMessage::new(
                header,
                DefaultRemotePartitionSyncRequest::decode_from(src, version)?,
            ))),
        }
    }
}
