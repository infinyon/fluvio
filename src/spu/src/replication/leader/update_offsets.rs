#![allow(clippy::assign_op_pattern)]

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use dataplane::Offset;
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

#[derive(Decoder, Encoder, Default, Debug)]
pub struct ReplicaOffsetRequest {
    pub replica: ReplicaKey,
    pub leo: Offset,
    pub hw: Offset,
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateOffsetResponse {}
