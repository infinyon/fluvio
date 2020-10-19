#![allow(clippy::assign_op_pattern)]

use std::fmt;
use std::hash::{Hash, Hasher};

use dataplane::api::Request;
use dataplane::derive::Decode;
use dataplane::derive::Encode;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_controlplane_metadata::partition::ReplicaStatus;

use crate::InternalScKey;

/// Live Replica Status
/// First lrs is leader by convention but should not be relied upon
#[derive(Decode, Encode, Debug, Default,Clone)]
pub struct UpdateLrsRequest {
    replicas: Vec<LrsRequest>
}

impl UpdateLrsRequest {
    pub fn new(replicas: Vec<LrsRequest> ) -> Self {
        Self {
            replicas
        }
    }

    /// make into vec of requests
    pub fn into_requests(self) -> Vec<LrsRequest> {
        self.replicas
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
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateLrsResponse {}




/// Request to update replica status
#[derive(Decode, Encode, Debug, Default,Clone)]
pub struct LrsRequest {
    pub id: ReplicaKey,
    pub leader: ReplicaStatus,
    pub replicas: Vec<ReplicaStatus>,
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
    pub fn new(id: ReplicaKey, leader: ReplicaStatus, replicas: Vec<ReplicaStatus>) -> Self {
        Self {
            id,
            leader,
            replicas,
        }
    }
}

