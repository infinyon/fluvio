#![allow(clippy::assign_op_pattern)]

//!
//! # Partition Spec
//!
//!
use fluvio_types::SpuId;
use fluvio_protocol::{Encoder, Decoder};

use crate::topic::{CleanupPolicy, CompressionAlgorithm, Deduplication, TopicSpec, TopicStorageConfig};

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
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub replicas: Vec<SpuId>,
    #[fluvio(min_version = 4)]
    pub cleanup_policy: Option<CleanupPolicy>,
    #[fluvio(min_version = 4)]
    pub storage: Option<TopicStorageConfig>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 6)]
    pub compression_type: CompressionAlgorithm,
    #[fluvio(min_version = 12)]
    pub deduplication: Option<Deduplication>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 13)]
    pub system: bool,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 14)]
    pub mirror: Option<PartitionMirrorConfig>,
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
    pub fn from_replicas(
        replicas: Vec<SpuId>,
        topic: &TopicSpec,
        mirror: Option<&PartitionMirrorConfig>,
    ) -> Self {
        let leader = if replicas.is_empty() { 0 } else { replicas[0] };

        Self {
            leader,
            replicas,
            mirror: mirror.cloned(),
            cleanup_policy: topic.get_clean_policy().cloned(),
            storage: topic.get_storage().cloned(),
            compression_type: topic.get_compression_type().clone(),
            deduplication: topic.get_deduplication().cloned(),
            system: topic.is_system(),
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

    pub fn mirror_string(&self) -> String {
        if let Some(mirror) = &self.mirror {
            mirror.external_cluster()
        } else {
            "".to_owned()
        }
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

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub enum PartitionMirrorConfig {
    #[fluvio(tag = 0)]
    Source(SourcePartitionConfig),
    #[fluvio(tag = 1)]
    Target(TargetPartitionConfig),
}

impl Default for PartitionMirrorConfig {
    fn default() -> Self {
        Self::Source(SourcePartitionConfig::default())
    }
}

impl PartitionMirrorConfig {
    pub fn source(&self) -> Option<&SourcePartitionConfig> {
        match self {
            Self::Source(source) => Some(source),
            _ => None,
        }
    }

    pub fn target(&self) -> Option<&TargetPartitionConfig> {
        match self {
            Self::Target(target) => Some(target),
            _ => None,
        }
    }

    pub fn external_cluster(&self) -> String {
        match self {
            Self::Source(source) => format!("{}:{}", source.upstream_cluster, source.target_spu),
            Self::Target(target) => target.remote_cluster.clone(),
        }
    }
}

impl std::fmt::Display for PartitionMirrorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PartitionMirrorConfig::Source(cfg) => write!(f, "{}", cfg),
            PartitionMirrorConfig::Target(cfg) => write!(f, "{}", cfg),
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TargetPartitionConfig {
    pub remote_cluster: String,
    pub source_replica: String,
}

impl std::fmt::Display for TargetPartitionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Target:{}", self.remote_cluster)
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SourcePartitionConfig {
    pub upstream_cluster: String,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub target_spu: SpuId,
}

impl std::fmt::Display for SourcePartitionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Source:{}:{}", self.upstream_cluster, self.target_spu)
    }
}
