#![allow(clippy::assign_op_pattern)]

//!
//! # Partition Spec
//!
//!
use fluvio_types::SpuId;
use fluvio_protocol::{Encoder, Decoder};

use crate::topic::{CleanupPolicy, TopicStorageConfig, TopicSpec, CompressionAlgorithm};

/// Spec for Partition
/// Each partition has replicas spread among SPU
/// one of replica is leader which is duplicated in the leader field
#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct PartitionSpec {
    pub leader: SpuId,
    pub replicas: Vec<SpuId>,
    #[fluvio(min_version = 4)]
    pub cleanup_policy: Option<CleanupPolicy>,
    #[fluvio(min_version = 4)]
    pub storage: Option<TopicStorageConfig>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 6)]
    pub compression_type: CompressionAlgorithm,
}

impl PartitionSpec {
    pub fn new(leader: SpuId, replicas: Vec<SpuId>) -> Self {
        Self {
            leader,
            replicas,
            ..Default::default()
        }
    }

    /// Create new partition spec from replica mapping with topic spec. This assume first replica is leader
    pub fn from_replicas(replicas: Vec<SpuId>, topic: &TopicSpec) -> Self {
        let leader = if replicas.is_empty() { 0 } else { replicas[0] };

        Self {
            leader,
            replicas,
            cleanup_policy: topic.get_clean_policy().cloned(),
            storage: topic.get_storage().cloned(),
            compression_type: topic.get_compression_type().clone(),
        }
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

impl From<Vec<SpuId>> for PartitionSpec {
    fn from(replicas: Vec<SpuId>) -> Self {
        if !replicas.is_empty() {
            Self::new(replicas[0], replicas)
        } else {
            Self::new(0, replicas)
        }
    }
}

/// Setting applied to a replica
#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct PartitionConfig {
    pub retention_time_seconds: Option<u32>,
}
