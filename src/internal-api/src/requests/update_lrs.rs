#![allow(clippy::assign_op_pattern)]

use std::fmt;

use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

use crate::InternalScKey;
use flv_metadata_cluster::partition::ReplicaKey;
use flv_metadata_cluster::partition::ReplicaStatus;

/// Live Replica Status
/// First lrs is leader by convention but should not be relied upon
#[derive(Decode, Encode, Debug, Default, PartialEq, Clone)]
pub struct UpdateLrsRequest {
    pub id: ReplicaKey,
    pub leader: ReplicaStatus,
    pub replicas: Vec<ReplicaStatus>,
}

impl fmt::Display for UpdateLrsRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LrsUpdate {}", self.id)
    }
}

impl UpdateLrsRequest {
    pub fn new(id: ReplicaKey, leader: ReplicaStatus, replicas: Vec<ReplicaStatus>) -> Self {
        Self {
            id,
            leader,
            replicas,
        }
    }
}

impl Request for UpdateLrsRequest {
    const API_KEY: u16 = InternalScKey::UpdateLrs as u16;
    type Response = UpdateLrsResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateLrsResponse {}
