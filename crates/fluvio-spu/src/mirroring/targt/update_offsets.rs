use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::mirroring::COMMON_MIRROR_VERSION;
use crate::replication::leader::ReplicaOffsetRequest;

use super::api_key::MirrorTargetApiEnum;

/// Update target's offset
pub(crate) type UpdateTargetOffsetRequest = ReplicaOffsetRequest;

impl Request for UpdateTargetOffsetRequest {
    const API_KEY: u16 = MirrorTargetApiEnum::UpdateTargetOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = UpdateTargetOffsetResponse;
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateTargetOffsetResponse {}
