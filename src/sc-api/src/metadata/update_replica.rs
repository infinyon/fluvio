use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_metadata::message::*;

use crate::objects::Metadata;
use crate::partition::PartitionSpec;

pub type ReplicaUpdate = Message<Metadata<PartitionSpec>>;

/// Changes in the Replica Specs
#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct UpdateReplicaResponse {
    pub epoch: i64,
    pub replicas: Vec<ReplicaUpdate>
}

impl std::fmt::Display for UpdateReplicaResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "partitions: {}:{}", self.epoch,self.replicas.len())
    }
}

impl UpdateReplicaResponse {
    pub fn new(epoch: i64, replica_msgs: Vec<ReplicaUpdate>) -> Self {
        Self {
            epoch,
            replicas: replica_msgs,
        }
    }
}
