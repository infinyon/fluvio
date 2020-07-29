use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::Request;
use flv_metadata_cluster::message::ReplicaMsg;
use flv_metadata_cluster::partition::Replica;
use crate::InternalSpuApi;

/// Changes to Replica Specs
#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateReplicaRequest {
    pub epoch: i64,
    pub changes: Vec<ReplicaMsg>,
    pub all: Vec<Replica>,
}

impl Request for UpdateReplicaRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateReplica as u16;
    type Response = UpdateReplicaResponse;
}

impl UpdateReplicaRequest {
    pub fn with_changes(epoch: i64, changes: Vec<ReplicaMsg>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<Replica>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateReplicaResponse {}
