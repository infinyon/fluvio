#![allow(clippy::assign_op_pattern)]

use fluvio_types::SpuId;
use fluvio_protocol::{link::ErrorCode, Decoder, Encoder};

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
            let external = mirror.external_cluster();
            match mirror {
                PartitionMirrorConfig::Remote(remote) => {
                    if remote.target {
                        format!("{}(from-home)", external)
                    } else {
                        format!("{}(to-home)", external)
                    }
                }

                PartitionMirrorConfig::Home(home) => {
                    if home.source {
                        format!("{}(to-remote)", external)
                    } else {
                        format!("{}(from-remote)", external)
                    }
                }
            }
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
    Remote(RemotePartitionConfig),
    #[fluvio(tag = 1)]
    Home(HomePartitionConfig),
}

impl Default for PartitionMirrorConfig {
    fn default() -> Self {
        Self::Remote(RemotePartitionConfig::default())
    }
}

impl PartitionMirrorConfig {
    pub fn remote(&self) -> Option<&RemotePartitionConfig> {
        match self {
            Self::Remote(e) => Some(e),
            _ => None,
        }
    }

    pub fn home(&self) -> Option<&HomePartitionConfig> {
        match self {
            Self::Home(c) => Some(c),
            _ => None,
        }
    }

    pub fn external_cluster(&self) -> String {
        match self {
            Self::Remote(r) => format!(
                "{}:{}:{}",
                r.home_cluster, r.home_spu_id, r.home_spu_endpoint
            ),
            Self::Home(h) => h.remote_cluster.clone(),
        }
    }

    #[deprecated(since = "0.29.1")]
    pub fn is_home_mirror(&self) -> bool {
        matches!(self, Self::Home(_))
    }

    /// check whether this mirror should accept traffic
    pub fn accept_traffic(&self) -> Option<ErrorCode> {
        match self {
            Self::Remote(r) => {
                if r.target {
                    Some(ErrorCode::MirrorProduceFromRemoteNotAllowed)
                } else {
                    None
                }
            }
            Self::Home(h) => {
                if h.source {
                    None
                } else {
                    Some(ErrorCode::MirrorProduceFromHome)
                }
            }
        }
    }
}

impl std::fmt::Display for PartitionMirrorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PartitionMirrorConfig::Remote(cfg) => write!(f, "{}", cfg),
            PartitionMirrorConfig::Home(cfg) => write!(f, "{}", cfg),
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct HomePartitionConfig {
    pub remote_cluster: String,
    pub remote_replica: String,
    // if this is set, home will be mirror instead of
    #[cfg_attr(
        feature = "use_serde",
        serde(default, skip_serializing_if = "crate::is_false")
    )]
    #[fluvio(min_version = 18)]
    pub source: bool,
}

impl std::fmt::Display for HomePartitionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.remote_cluster)
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct RemotePartitionConfig {
    pub home_cluster: String,
    pub home_spu_key: String,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub home_spu_id: SpuId,
    pub home_spu_endpoint: String,
    #[cfg_attr(
        feature = "use_serde",
        serde(default, skip_serializing_if = "crate::is_false")
    )]
    #[fluvio(min_version = 18)]
    pub target: bool,
}

impl std::fmt::Display for RemotePartitionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.home_cluster, self.home_spu_id, self.home_spu_endpoint
        )
    }
}
