use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::Request;
use flv_metadata::api::Message;

use super::ScClientApiKey;
use super::replica::ReplicaLeader;

pub type ReplicaMsg = Message<ReplicaLeader>;

/// Changes in the Replica Specs
#[derive(Decode, Encode, Debug, Default)]
pub struct ReplicaChangeRequest {
    pub replicas: Vec<ReplicaMsg>,
}

impl Request for ReplicaChangeRequest {
    const API_KEY: u16 = ScClientApiKey::ReplicaChange as u16;
    type Response = ReplicaChangeResponse;
}

impl ReplicaChangeRequest {
    pub fn new(replica_msgs: Vec<ReplicaMsg>) -> Self {
        Self {
            replicas: replica_msgs,
        }
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaChangeResponse {}
