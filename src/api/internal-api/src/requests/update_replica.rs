use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::Request;

use crate::messages::ReplicaMsgs;
use crate::InternalSpuApi;

/// Changes in the Replica Specs
#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateReplicaRequest {
    replicas: ReplicaMsgs,
}

impl Request for UpdateReplicaRequest{
    const API_KEY: u16 = InternalSpuApi::UpdateReplica as u16;
    type Response = UpdateReplicaResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateReplicaResponse {}

impl UpdateReplicaRequest {
    pub fn encode_request(replica_msgs: ReplicaMsgs) -> Self {
        UpdateReplicaRequest {
            replicas: replica_msgs,
        }
    }

    pub fn decode_request(&self) -> &ReplicaMsgs {
        &self.replicas
    }

    pub fn replicas(self) -> ReplicaMsgs {
        self.replicas
    }
}
