#![allow(clippy::assign_op_pattern)]

use kf_protocol::derive::{Decode, Encode};

#[derive(Encode, Decode, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SpuGroupSpec {
    /// The number of replicas for the spu group
    pub replicas: u16,

    /// The base spu id that the spu group uses to increment the spu ids
    /// Note: Spu id is a globally unique resource and it cannot be shared
    pub min_id: i32,

    /// Configuration elements to be applied to each SPUs in the group
    pub spu_config: SpuConfig,
}

#[derive(Encode, Decode, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SpuConfig {
    pub rack: Option<String>,
    pub replication: Option<ReplicationConfig>,
    pub storage: Option<StorageConfig>,
    pub env: Vec<EnvVar>,
}

impl SpuConfig {
    pub fn real_storage_config(&self) -> RealStorageConfig {
        if let Some(config) = &self.storage {
            config.real_config()
        } else {
            StorageConfig::default().real_config()
        }
    }
}

#[derive(Encode, Decode, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ReplicationConfig {
    pub in_sync_replica_min: Option<u16>,
}

#[derive(Encode, Decode, Debug, Default, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct StorageConfig {
    pub log_dir: Option<String>,
    pub size: Option<String>,
}

impl StorageConfig {
    /// fill in the values if not defined
    /// that should be used
    pub fn real_config(&self) -> RealStorageConfig {
        RealStorageConfig {
            log_dir: self
                .log_dir
                .clone()
                .unwrap_or_else(|| "/tmp/fluvio".to_owned()),
            size: self.size.clone().unwrap_or_else(|| "1Gi".to_owned()),
        }
    }
}

/// real storage configuration
pub struct RealStorageConfig {
    pub log_dir: String,
    pub size: String,
}

#[derive(Encode, Decode, Default, Debug, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}
