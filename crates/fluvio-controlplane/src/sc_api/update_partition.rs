use std::fmt;
use std::hash::{Hash, Hasher};

use fluvio_controlplane_metadata::partition::PartitionResolution;
use fluvio_protocol::api::Request;
use fluvio_protocol::record::ReplicaKey;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use super::api::InternalScKey;

/// Live Partition Status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct UpdatePartitionStatRequest {
    stats: Vec<PartitionStatRequest>,
}

impl UpdatePartitionStatRequest {
    pub fn new(stats: Vec<PartitionStatRequest>) -> Self {
        Self { stats }
    }

    /// make into vec of requests
    pub fn into_stats(self) -> Vec<PartitionStatRequest> {
        self.stats
    }
}

impl fmt::Display for UpdatePartitionStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mirror updates {}", self.stats.len())
    }
}

impl Request for UpdatePartitionStatRequest {
    const API_KEY: u16 = InternalScKey::UpdatePartition as u16;
    type Response = UpdatePartitionResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdatePartitionResponse {}

/// Request to update replica status
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct PartitionStatRequest {
    pub replica_key: ReplicaKey,
    pub resolution: PartitionResolution,
}

impl PartialEq for PartitionStatRequest {
    fn eq(&self, other: &Self) -> bool {
        self.replica_key == other.replica_key
    }
}

impl Eq for PartitionStatRequest {}

// we only care about id for hashing
impl Hash for PartitionStatRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.replica_key.hash(state);
    }
}

impl fmt::Display for PartitionStatRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionUpdate {}", self.replica_key)
    }
}

impl PartitionStatRequest {
    pub fn new(key: ReplicaKey, resolution: PartitionResolution) -> Self {
        Self {
            replica_key: key,
            resolution,
        }
    }
}
