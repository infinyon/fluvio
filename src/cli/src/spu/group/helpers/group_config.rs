//!
//! # Group Config
//!
//! Group configuration is read from file
//!

use serde::Deserialize;

use sc_api::server::spu::*;

#[derive(Debug, Deserialize, Default, PartialEq)]
pub struct GroupConfig {
    pub storage: Option<StorageConfig>,
    pub replication: Option<ReplicationConfig>,
    pub env: Vec<EnvVar>,
}

impl GroupConfig {
    pub fn with_storage(storage: String) -> Self {
        Self {
            storage: Some(StorageConfig::new(storage)),
            ..Default::default()
        }
    }
}

impl Into<FlvGroupConfig> for GroupConfig {
    fn into(self) -> FlvGroupConfig {
        FlvGroupConfig {
            storage: self.storage.map(|cfg| cfg.into()),
            replication: self.replication.map(|cfg| cfg.into()),
            env: self.env.into_iter().map(|var| var.into()).collect(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StorageConfig {
    pub log_dir: Option<String>,
    pub size: Option<String>,
}

impl StorageConfig {
    pub fn new(size: String) -> Self {
        Self {
            log_dir: None,
            size: Some(size),
        }
    }
}

impl Into<FlvStorageConfig> for StorageConfig {
    fn into(self) -> FlvStorageConfig {
        FlvStorageConfig {
            log_dir: self.log_dir.map(|cfg| cfg.into()),
            size: self.size.map(|cfg| cfg.into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationConfig {
    pub in_sync_replica_min: Option<u16>,
}

impl Into<FlvReplicationConfig> for ReplicationConfig {
    fn into(self) -> FlvReplicationConfig {
        FlvReplicationConfig {
            in_sync_replica_min: self.in_sync_replica_min.map(|cfg| cfg.into()),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

impl Into<FlvEnvVar> for EnvVar {
    fn into(self) -> FlvEnvVar {
        FlvEnvVar {
            name: self.name,
            value: self.value,
        }
    }
}
