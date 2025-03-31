use std::io::Error as IoError;
use std::convert::TryInto;

use tracing::{info, trace};

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Decoder;
use fluvio_protocol::api::{RequestMessage, ApiMessage, RequestHeader};

use super::api_key::FollowerPeerApiEnum;
use super::sync::DefaultSyncRequest;
use super::reject_request::RejectOffsetRequest;

#[derive(Debug)]
pub enum FollowerPeerRequest {
    SyncRecords(RequestMessage<DefaultSyncRequest>),
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
            FollowerPeerApiEnum::SyncRecords => {
                info!("decoding sync records header: {:#?}", header);


                let len = src.remaining();
                info!("decoding sync records len: {len}");


                match DefaultSyncRequest::decode_from(src, version) {
                    Ok(request) => {
                        trace!("decoding sync records request: {:#?}", request);
                        Ok(FollowerPeerRequest::SyncRecords(RequestMessage::new(
                            header, request,
                        )))
                    }
                    Err(e) => {
                        info!("failed to decode sync records request: {:#?}", e);
                        Err(e)
                    }
                }
            }
            FollowerPeerApiEnum::RejectedOffsetRequest => {
                Ok(FollowerPeerRequest::RejectedOffsetRequest(
                    RequestMessage::new(header, RejectOffsetRequest::decode_from(src, version)?),
                ))
            }
        }
    }
}
