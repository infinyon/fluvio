#![allow(clippy::assign_op_pattern)]

use dataplane::derive::{Decode, Encode};
use dataplane::api::Request;
use dataplane::Offset;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::LeaderPeerApiEnum;

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateOffsetRequest {
    pub replicas: Vec<ReplicaOffsetRequest>,
}

impl Request for UpdateOffsetRequest {
    const API_KEY: u16 = LeaderPeerApiEnum::UpdateOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = UpdateOffsetResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaOffsetRequest {
    pub replica: ReplicaKey,
    pub leo: Offset,
    pub hw: Offset,
}

// no content, this is one way request
#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateOffsetResponse {}
