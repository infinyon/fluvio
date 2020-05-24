use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::Decoder;
use kf_protocol::Encoder;
use kf_protocol::api::Request;

use crate::messages::LeaderMsgs;
use crate::InternalKfApiKey;


#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateReplicaLeaderRequest {
    leaders: LeaderMsgs,
}

impl Request for UpdateReplicaLeaderRequest{
    const API_KEY: u16 = InternalKfApiKey::UpdateReplicaLeaders as u16;
    type Response = UpdateReplicaLeaderResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateReplicaLeaderResponse {}

impl UpdateReplicaLeaderRequest {

    pub fn leaders(self) -> LeaderMsgs {
        self.leaders
    }
    
    pub fn encode_request(leader_msgs: LeaderMsgs) -> Self {
        UpdateReplicaLeaderRequest {
            leaders: leader_msgs,
        }
    }

    pub fn decode_request(&self) -> &LeaderMsgs {
        &self.leaders
    }
}
