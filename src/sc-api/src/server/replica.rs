use kf_protocol::derive::*;
use kf_protocol::api::ReplicaKey;
use flv_types::SpuId;

/// given replica, where is leader
#[derive(Decode, Encode, Debug, PartialEq, Clone, Default)]
pub struct ReplicaLeader {
    pub id: ReplicaKey,
    pub leader: SpuId,
}
