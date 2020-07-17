use std::fmt;

use kf_protocol::derive::{Decode, Encode};
use flv_types::SpuId;
use crate::partition::ReplicaKey;
use crate::core::*;
use super::store::*;

#[derive(Decode, Encode, Debug, PartialEq, Clone, Default)]
pub struct Replica {
    pub id: ReplicaKey,
    pub leader: SpuId,
    pub replicas: Vec<SpuId>,
}

impl Replica {
    pub fn new(id: ReplicaKey, leader: SpuId, replicas: Vec<SpuId>) -> Self {
        Replica {
            id,
            leader,
            replicas,
        }
    }
}

impl<C> From<PartitionMetadata<C>> for Replica
where
    C: MetadataItem,
{
    fn from(item: PartitionMetadata<C>) -> Self {
        Self {
            id: item.key,
            leader: item.spec.leader,
            replicas: item.spec.replicas,
        }
    }
}

impl fmt::Display for Replica {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} leader: {} replicas: [", self.id, self.leader)?;
        for replica in &self.replicas {
            write!(f, "{},", replica)?;
        }
        write!(f, "]")
    }
}

/// given replica, where is leader
#[derive(Decode, Encode, Debug, PartialEq, Clone, Default)]
pub struct ReplicaLeader {
    pub id: ReplicaKey,
    pub leader: SpuId,
}
