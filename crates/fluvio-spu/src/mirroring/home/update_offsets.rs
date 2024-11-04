use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::mirroring::COMMON_MIRROR_VERSION;
use crate::replication::leader::ReplicaOffsetRequest;

use super::api_key::MirrorHomeApiEnum;

/// Update home's offset to remote
pub(crate) type UpdateHomeOffsetRequest = ReplicaOffsetRequest;

impl Request for UpdateHomeOffsetRequest {
    const API_KEY: u16 = MirrorHomeApiEnum::UpdateHomeOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = UpdateHomeOffsetResponse;
}

// no content, this is one way request
#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateHomeOffsetResponse {}
