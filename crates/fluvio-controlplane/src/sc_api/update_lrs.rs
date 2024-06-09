use std::fmt;
use std::hash::{Hash, Hasher};

use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_controlplane_metadata::partition::ReplicaStatus;

use super::api::InternalScKey;

/// Live Replica Status
/// First lrs is leader by convention but should not be relied upon
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct UpdateLrsRequest {
    replicas: Vec<LrsRequest>,
    stat: SpuStat,
}

impl UpdateLrsRequest {
    pub fn new(replicas: Vec<LrsRequest>) -> Self {
        Self {
            replicas,
            stat: SpuStat::default(),
        }
    }

    /// make into vec of requests
    pub fn into_requests(self) -> Vec<LrsRequest> {
        self.replicas
    }

    pub fn stat(self) -> SpuStat {
        self.stat
    }
}

impl fmt::Display for UpdateLrsRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "lrs updates {}", self.replicas.len())
    }
}

impl Request for UpdateLrsRequest {
    const API_KEY: u16 = InternalScKey::UpdateLrs as u16;
    type Response = UpdateLrsResponse;
    const DEFAULT_API_VERSION: i16 = 1;
}

#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct SpuStat {
    smartmodules_count: u32,
    smart_streams_count: u32,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateLrsResponse {}

/// Request to update replica status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct LrsRequest {
    pub id: ReplicaKey,
    pub leader: ReplicaStatus,
    pub replicas: Vec<ReplicaStatus>,
    pub size: i64,
    #[fluvio(min_version = 1)]
    pub base_offset: i64,
}

impl PartialEq for LrsRequest {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for LrsRequest {}

// we only care about id for hashing
impl Hash for LrsRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl fmt::Display for LrsRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LrsUpdate {}", self.id)
    }
}

impl LrsRequest {
    pub fn new(
        id: ReplicaKey,
        leader: ReplicaStatus,
        replicas: Vec<ReplicaStatus>,
        size: i64,
        base_offset: i64,
    ) -> Self {
        Self {
            id,
            leader,
            replicas,
            size,
            base_offset,
        }
    }
}
