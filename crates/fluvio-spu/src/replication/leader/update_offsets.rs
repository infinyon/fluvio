#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_protocol::record::Offset;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::LeaderPeerApiEnum;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateOffsetRequest {
    pub replicas: Vec<ReplicaOffsetRequest>,
}

impl Request for UpdateOffsetRequest {
    const API_KEY: u16 = LeaderPeerApiEnum::UpdateOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = UpdateOffsetResponse;
}

#[derive(Decoder, Encoder, Default, Clone, Debug)]
pub struct ReplicaOffsetRequest {
    pub replica: ReplicaKey,
    pub leo: Offset,
    pub hw: Offset,
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateOffsetResponse {}
