use std::fmt;
use std::hash::{Hash, Hasher};

use fluvio_controlplane_metadata::spu::SpuStatus;
use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use super::api::InternalScKey;

/// Live Spu Status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct UpdateSpuStatRequest {
    stats: Vec<SpuStatRequest>,
}

impl UpdateSpuStatRequest {
    pub fn new(stats: Vec<SpuStatRequest>) -> Self {
        Self { stats }
    }

    /// make into vec of requests
    pub fn into_stats(self) -> Vec<SpuStatRequest> {
        self.stats
    }
}

impl fmt::Display for UpdateSpuStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mirror updates {}", self.stats.len())
    }
}

impl Request for UpdateSpuStatRequest {
    const API_KEY: u16 = InternalScKey::UpdateSpu as u16;
    type Response = UpdateSpuResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSpuResponse {}

/// Request to update replica status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct SpuStatRequest {
    pub spu_id: i32,
    pub status: SpuStatus,
}

impl PartialEq for SpuStatRequest {
    fn eq(&self, other: &Self) -> bool {
        self.spu_id == other.spu_id
    }
}

impl Eq for SpuStatRequest {}

// we only care about id for hashing
impl Hash for SpuStatRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.spu_id.hash(state);
    }
}

impl fmt::Display for SpuStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpuUpdate {}", self.spu_id)
    }
}

impl SpuStatRequest {
    pub fn new(id: i32, status: SpuStatus) -> Self {
        Self { spu_id: id, status }
    }
}
