use std::default::Default;
use std::path::PathBuf;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use derive_builder::Builder;
use serde::Deserialize;

use fluvio_types::defaults::{SPU_LOG_INDEX_MAX_BYTES, SPU_LOG_BASE_DIR};
use fluvio_types::defaults::SPU_LOG_INDEX_MAX_INTERVAL_BYTES;
use fluvio_types::defaults::SPU_LOG_SEGMENT_MAX_BYTES;
use dataplane::Size;

use crate::ReplicaStorageConfig;

pub const DEFAULT_FLUSH_WRITE_COUNT: u32 = 1;
pub const DEFAULT_FLUSH_IDLE_MSEC: u32 = 0;
pub const DEFAULT_MAX_BATCH_SIZE: u32 = 1048588;
pub const DEFAULT_RETENTION_SECONDS: u32 = 7 * 24 * 3600;

// common option
#[derive(Builder, Clone, Debug, PartialEq, Deserialize)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ReplicaConfig {
    #[builder(default = "default_base_dir()")]
    #[serde(default = "default_base_dir")]
    pub base_dir: PathBuf,
    #[builder(default = "default_index_max_bytes()")]
    #[serde(default = "default_index_max_bytes")]
    pub index_max_bytes: Size,
    #[builder(default = "default_index_max_interval_bytes()")]
    #[serde(default = "default_index_max_interval_bytes")]
    pub index_max_interval_bytes: Size,
    #[builder(default = "default_segment_max_bytes()")]
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: Size,
    #[builder(default = "default_flush_write_count()")]
    #[serde(default = "default_flush_write_count")]
    pub flush_write_count: Size,
    #[builder(default = "default_flush_idle_msec()")]
    #[serde(default = "default_flush_idle_msec")]
    pub flush_idle_msec: Size,
    #[builder(default = "default_max_batch_size()")]
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: Size,
    #[builder(default = "default_update_hw()")]
    #[serde(default = "default_update_hw")]
    pub update_hw: bool, // if true, enable hw update
    #[builder(default = "default_retention_seconds()")]
    #[serde(default = "default_retention_seconds")]
    pub retention_seconds: Size,
}

impl fmt::Display for ReplicaConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "storage config at: {:#?}", self.base_dir)
    }
}

impl ReplicaStorageConfig for ReplicaConfig {}

fn default_base_dir() -> PathBuf {
    PathBuf::from(SPU_LOG_BASE_DIR)
}

const fn default_update_hw() -> bool {
    true
}

const fn default_index_max_bytes() -> Size {
    SPU_LOG_INDEX_MAX_BYTES
}

const fn default_index_max_interval_bytes() -> Size {
    SPU_LOG_INDEX_MAX_INTERVAL_BYTES
}

const fn default_segment_max_bytes() -> Size {
    SPU_LOG_SEGMENT_MAX_BYTES
}

const fn default_flush_write_count() -> Size {
    DEFAULT_FLUSH_WRITE_COUNT
}

const fn default_flush_idle_msec() -> Size {
    DEFAULT_FLUSH_IDLE_MSEC
}

const fn default_max_batch_size() -> Size {
    DEFAULT_MAX_BATCH_SIZE
}

const fn default_retention_seconds() -> Size {
    DEFAULT_RETENTION_SECONDS
}

impl ReplicaConfig {
    // Used to get a [`ConfigOptionBuilder`].
    pub fn builder() -> ReplicaConfigBuilder {
        ReplicaConfigBuilder::default()
    }

    pub fn shared(self) -> Arc<SharedReplicaConfig> {
        Arc::new(self.into())
    }
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            base_dir: default_base_dir(),
            index_max_bytes: default_index_max_bytes(),
            index_max_interval_bytes: default_index_max_interval_bytes(),
            segment_max_bytes: default_segment_max_bytes(),
            flush_write_count: default_flush_write_count(),
            flush_idle_msec: default_flush_idle_msec(),
            max_batch_size: default_max_batch_size(),
            retention_seconds: default_retention_seconds(),
            update_hw: true,
        }
    }
}

impl ReplicaConfigBuilder {
    /// Build a [`ConfigOption`] with the current
    /// values in [`ConfigOptionBuilder`]
    pub fn build(&self) -> ReplicaConfig {
        // This will not fail as long as all fields
        // of ConfigOption have a #[builder(default = ..) attribute.
        self.build_impl()
            .expect("Error builing ConfigOption struct")
    }
}

#[derive(Debug)]
pub struct SharedConfigValue(AtomicU32);

impl SharedConfigValue {
    pub fn new(value: u32) -> Self {
        SharedConfigValue(AtomicU32::new(value))
    }

    #[inline(always)]
    pub fn get(&self) -> u32 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set(&self, value: u32) {
        self.0.store(value, std::sync::atomic::Ordering::Relaxed)
    }
}

/// Config that can be shared updated
#[derive(Debug)]
pub struct SharedReplicaConfig {
    pub base_dir: PathBuf,
    pub index_max_bytes: SharedConfigValue,
    pub index_max_interval_bytes: SharedConfigValue,
    pub segment_max_bytes: SharedConfigValue,
    pub flush_write_count: SharedConfigValue,
    pub flush_idle_msec: SharedConfigValue,
    pub max_batch_size: SharedConfigValue,
    pub update_hw: bool, // if true, enable hw update
    pub retention_seconds: SharedConfigValue,
}

impl From<ReplicaConfig> for SharedReplicaConfig {
    fn from(config: ReplicaConfig) -> Self {
        SharedReplicaConfig {
            base_dir: config.base_dir,
            index_max_bytes: SharedConfigValue::new(config.index_max_bytes),
            index_max_interval_bytes: SharedConfigValue::new(config.index_max_interval_bytes),
            segment_max_bytes: SharedConfigValue::new(config.segment_max_bytes),
            flush_write_count: SharedConfigValue::new(config.flush_write_count),
            flush_idle_msec: SharedConfigValue::new(config.flush_idle_msec),
            max_batch_size: SharedConfigValue::new(config.max_batch_size),
            update_hw: config.update_hw,
            retention_seconds: SharedConfigValue::new(config.retention_seconds),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_builder() {
        let config: ReplicaConfig = ReplicaConfig::builder().build();

        assert_eq!(ReplicaConfig::default(), config);
    }
}
