#![allow(clippy::assign_op_pattern)]

use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::Request;
use kf_protocol::api::Offset;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::KfLeaderPeerApiEnum;

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateOffsetRequest {
    pub replicas: Vec<ReplicaOffsetRequest>,
}

impl Request for UpdateOffsetRequest {
    const API_KEY: u16 = KfLeaderPeerApiEnum::UpdateOffsets as u16;
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
