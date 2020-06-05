use std::io::Error as IoError;
use std::convert::TryInto;

use log::trace;

use kf_protocol::bytes::Buf;
use kf_protocol::Decoder;
use kf_protocol::derive::Encode;

use kf_protocol::api::KfRequestMessage;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;

use super::KfFollowerPeerApiEnum;
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

impl KfRequestMessage for FollowerPeerRequest {
    type ApiKey = KfFollowerPeerApiEnum;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        let version = header.api_version();
        match header.api_key().try_into()? {
            KfFollowerPeerApiEnum::SyncRecords => Ok(FollowerPeerRequest::SyncRecords(
                RequestMessage::new(header, DefaultSyncRequest::decode_from(src, version)?),
            )),
        }
    }
}
