use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::MirrorSourceApiEnum;
use super::sync::DefaultPartitionSyncRequest;

#[derive(Debug, Encoder)]
pub enum SourceMirrorRequest {
    #[fluvio(tag = 0)]
    SyncRecords(RequestMessage<DefaultPartitionSyncRequest>),
    //   #[fluvio(tag = 1)]
    //   RejectedOffsetRequest(RequestMessage<RejectOffsetRequest>),
}

impl Default for SourceMirrorRequest {
    fn default() -> Self {
        Self::SyncRecords(RequestMessage::<DefaultPartitionSyncRequest>::default())
    }
}

impl ApiMessage for SourceMirrorRequest {
    type ApiKey = MirrorSourceApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            MirrorSourceApiEnum::SyncRecords => Ok(Self::SyncRecords(RequestMessage::new(
                header,
                DefaultPartitionSyncRequest::decode_from(src, version)?,
            ))),
        }
    }
}
