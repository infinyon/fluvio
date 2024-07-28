use std::fmt;
use std::hash::{Hash, Hasher};

use fluvio_controlplane_metadata::mirror::MirrorStatus;
use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use super::api::InternalScKey;

/// Live Mirror Status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct UpdateMirrorStatRequest {
    stats: Vec<MirrorStatRequest>,
}

impl UpdateMirrorStatRequest {
    pub fn new(stats: Vec<MirrorStatRequest>) -> Self {
        Self { stats }
    }

    /// make into vec of requests
    pub fn into_stats(self) -> Vec<MirrorStatRequest> {
        self.stats
    }
}

impl fmt::Display for UpdateMirrorStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mirror updates {}", self.stats.len())
    }
}

impl Request for UpdateMirrorStatRequest {
    const API_KEY: u16 = InternalScKey::UpdateMirror as u16;
    type Response = UpdateMirrorResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateMirrorResponse {}

/// Request to update replica status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct MirrorStatRequest {
    pub mirror_id: String,
    pub status: MirrorStatus,
}

impl PartialEq for MirrorStatRequest {
    fn eq(&self, other: &Self) -> bool {
        self.mirror_id == other.mirror_id
    }
}

impl Eq for MirrorStatRequest {}

// we only care about id for hashing
impl Hash for MirrorStatRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.mirror_id.hash(state);
    }
}

impl fmt::Display for MirrorStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MirrorUpdate {}", self.mirror_id)
    }
}

impl MirrorStatRequest {
    pub fn new(id: String, status: MirrorStatus) -> Self {
        Self {
            mirror_id: id,
            status,
        }
    }
}
