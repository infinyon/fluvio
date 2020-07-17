//!
//! # Partition Spec
//!
//!
use flv_types::SpuId;
use kf_protocol::derive::{Decode, Encode};


/// Spec for Partition
/// Each partition has replicas spread among SPU
/// one of replica is leader which is duplicated in the leader field
#[derive(Decode, Encode, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize,serde::Deserialize),serde(rename_all = "camelCase"))]
pub struct PartitionSpec {
    pub leader: SpuId,
    pub replicas: Vec<SpuId>,
}

impl std::default::Default for PartitionSpec {
    fn default() -> Self {
        PartitionSpec {
            leader: 0,
            replicas: Vec::default(),
        }
    }
}


impl PartitionSpec {
    pub fn new(leader: SpuId, replicas: Vec<SpuId>) -> Self {
        Self { leader, replicas }
    }

    pub fn has_spu(&self, spu: &SpuId) -> bool {
        self.replicas.contains(spu)
    }

    /// follower replicas
    pub fn followers(&self) -> Vec<SpuId> {
       
        self.replicas.iter().filter_map(|r| if r == &self.leader { None} else { Some(*r)} ).collect()
    }
}

impl From<Vec<i32>> for PartitionSpec {
    fn from(replicas: Vec<i32>) -> Self {
        if replicas.len() > 0 {
            Self::new(replicas[0].clone(), replicas)
        } else {
            Self::new(0, replicas)
        }
    }
}

