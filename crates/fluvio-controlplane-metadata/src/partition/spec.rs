#![allow(clippy::assign_op_pattern)]

//!
//! # Partition Spec
//!
//!
use fluvio_types::SpuId;
use dataplane::core::{Encoder, Decoder};

/// Spec for Partition
/// Each partition has replicas spread among SPU
/// one of replica is leader which is duplicated in the leader field
#[derive(Decoder, Encoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct PartitionSpec {
    pub leader: SpuId,
    pub replicas: Vec<SpuId>,
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
        self.replicas
            .iter()
            .filter_map(|r| if r == &self.leader { None } else { Some(*r) })
            .collect()
    }
}

impl From<Vec<i32>> for PartitionSpec {
    fn from(replicas: Vec<i32>) -> Self {
        if !replicas.is_empty() {
            Self::new(replicas[0], replicas)
        } else {
            Self::new(0, replicas)
        }
    }
}

/// Setting applied to a replica
#[derive(Decoder, Encoder, Debug, PartialEq, Clone, Default)]
pub struct PartitionConfig {
    pub retention_time_seconds: Option<u32>,
}
