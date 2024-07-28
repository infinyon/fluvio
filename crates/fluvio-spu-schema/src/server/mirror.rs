use fluvio_protocol::{Decoder, Encoder, api::Request};

use crate::COMMON_VERSION;

use super::SpuServerApiKey;

/// Request to start mirror request
/// After this, SPU to SPU will use internal mirror protocol
/// This should be moved to Fluvio
#[derive(Decoder, Encoder, Default, Debug)]
pub struct StartMirrorRequest {
    pub remote_replica: String,
    pub remote_cluster_id: String,
}

impl Request for StartMirrorRequest {
    const API_KEY: u16 = SpuServerApiKey::StartMirror as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = StartMirrorResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct StartMirrorResponse;
