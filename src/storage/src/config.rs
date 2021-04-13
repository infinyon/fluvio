use std::default::Default;
use std::path::PathBuf;
use std::fmt;

use serde::Deserialize;

use fluvio_types::defaults::{SPU_LOG_INDEX_MAX_BYTES, SPU_LOG_BASE_DIR};
use fluvio_types::defaults::SPU_LOG_INDEX_MAX_INTERVAL_BYTES;
use fluvio_types::defaults::SPU_LOG_SEGMENT_MAX_BYTES;
use dataplane::Size;

use crate::ReplicaStorageConfig;

pub const DEFAULT_FLUSH_WRITE_COUNT: u32 = 1;
pub const DEFAULT_FLUSH_IDLE_MSEC: u32 = 0;
pub const DEFAULT_MAX_BATCH_SIZE: u32 = 1048588;

// common option
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ConfigOption {
    #[serde(default = "default_base_dir")]
    pub base_dir: PathBuf,
    #[serde(default = "default_index_max_bytes")]
    pub index_max_bytes: Size,
    #[serde(default = "default_index_max_interval_bytes")]
    pub index_max_interval_bytes: Size,
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: Size,
    #[serde(default = "default_flush_write_count")]
    pub flush_write_count: Size,
    #[serde(default = "default_flush_idle_msec")]
    pub flush_idle_msec: Size,
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: Size,
    #[serde(default = "default_update_hw")]
    pub update_hw: bool, // if true, enable hw update
}

impl fmt::Display for ConfigOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "storage config at: {:#?}", self.base_dir)
    }
}

impl ReplicaStorageConfig for ConfigOption {}

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

impl ConfigOption {
    pub fn new(
        base_dir: PathBuf,
        index_max_bytes: Size,
        index_max_interval_bytes: Size,
        segment_max_bytes: Size,
        flush_write_count: Size,
        flush_idle_msec: Size,
        max_batch_size: Size,
    ) -> Self {
        ConfigOption {
            base_dir,
            index_max_bytes,
            index_max_interval_bytes,
            segment_max_bytes,
            flush_write_count,
            flush_idle_msec,
            max_batch_size,
            update_hw: true,
        }
    }

    pub fn base_dir(mut self, dir: PathBuf) -> Self {
        self.base_dir = dir;
        self
    }

    pub fn index_max_bytes(mut self, bytes: Size) -> Self {
        self.index_max_bytes = bytes;
        self
    }

    pub fn segment_max_bytes(mut self, bytes: Size) -> Self {
        self.segment_max_bytes = bytes;
        self
    }

    /// disable hw update
    pub fn disable_update_hw(mut self) -> Self {
        self.update_hw = false;
        self
    }
}

impl Default for ConfigOption {
    fn default() -> Self {
        ConfigOption {
            base_dir: default_base_dir(),
            index_max_bytes: default_index_max_bytes(),
            index_max_interval_bytes: default_index_max_interval_bytes(),
            segment_max_bytes: default_segment_max_bytes(),
            flush_write_count: default_flush_write_count(),
            flush_idle_msec: default_flush_idle_msec(),
            max_batch_size: default_max_batch_size(),
            update_hw: true,
        }
    }
}
