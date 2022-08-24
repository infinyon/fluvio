#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::api_key::FollowerPeerApiEnum;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct RejectOffsetRequest {
    pub replicas: Vec<ReplicaKey>,
}

impl Request for RejectOffsetRequest {
    const API_KEY: u16 = FollowerPeerApiEnum::RejectedOffsetRequest as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = RejectOffsetResponse;
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct RejectOffsetResponse {}
