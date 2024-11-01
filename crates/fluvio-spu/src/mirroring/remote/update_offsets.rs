use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::mirroring::COMMON_MIRROR_VERSION;
use crate::replication::leader::ReplicaOffsetRequest;

use super::api_key::MirrorRemoteApiEnum;

/// Edge offset
#[derive(Decoder, Encoder, Default, Clone, Debug)]
pub(crate) struct UpdateEdgeOffsetRequest(ReplicaOffsetRequest);

impl From<ReplicaOffsetRequest> for UpdateEdgeOffsetRequest {
    fn from(offset: ReplicaOffsetRequest) -> Self {
        Self(offset)
    }
}

impl UpdateEdgeOffsetRequest {
    pub fn offset(&self) -> &ReplicaOffsetRequest {
        &self.0
    }
}

impl Request for UpdateEdgeOffsetRequest {
    const API_KEY: u16 = MirrorRemoteApiEnum::UpdateEdgeOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = UpdateEdgeOffsetResponse;
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateEdgeOffsetResponse {}
