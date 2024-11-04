use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::mirroring::COMMON_MIRROR_VERSION;
use crate::replication::leader::ReplicaOffsetRequest;

use super::api_key::MirrorRemoteApiEnum;

/// Edge offset
#[derive(Decoder, Encoder, Default, Clone, Debug)]
pub(crate) struct UpdateRemoteOffsetRequest(ReplicaOffsetRequest);

impl From<ReplicaOffsetRequest> for UpdateRemoteOffsetRequest {
    fn from(offset: ReplicaOffsetRequest) -> Self {
        Self(offset)
    }
}

impl UpdateRemoteOffsetRequest {
    pub fn offset(&self) -> &ReplicaOffsetRequest {
        &self.0
    }
}

impl Request for UpdateRemoteOffsetRequest {
    const API_KEY: u16 = MirrorRemoteApiEnum::UpdateEdgeOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = UpdateEdgeOffsetResponse;
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateEdgeOffsetResponse {}
