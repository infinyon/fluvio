use std::default::Default;
use std::path::PathBuf;
use std::fmt;

use serde::Deserialize;

use fluvio_types::defaults::{SPU_LOG_INDEX_MAX_BYTES, SPU_LOG_BASE_DIR};
use fluvio_types::defaults::SPU_LOG_INDEX_MAX_INTERVAL_BYTES;
use fluvio_types::defaults::SPU_LOG_SEGMENT_MAX_BYTES;

use dataplane::Size;

pub const DEFAULT_FLUSH_WRITE_COUNT: u32 = 1;

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
}

impl fmt::Display for ConfigOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "storage config at: {:#?}", self.base_dir)
    }
}

fn default_base_dir() -> PathBuf {
    PathBuf::from(SPU_LOG_BASE_DIR)
}

fn default_index_max_bytes() -> Size {
    SPU_LOG_INDEX_MAX_BYTES
}

fn default_index_max_interval_bytes() -> Size {
    SPU_LOG_INDEX_MAX_INTERVAL_BYTES
}

fn default_segment_max_bytes() -> Size {
    SPU_LOG_SEGMENT_MAX_BYTES
}

fn default_flush_write_count() -> Size {
    DEFAULT_FLUSH_WRITE_COUNT
}

impl ConfigOption {
    pub fn new(
        base_dir: PathBuf,
        index_max_bytes: u32,
        index_max_interval_bytes: u32,
        segment_max_bytes: u32,
        flush_write_count: u32,
    ) -> Self {
        ConfigOption {
            base_dir,
            index_max_bytes,
            index_max_interval_bytes,
            segment_max_bytes,
            flush_write_count,
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
}

impl Default for ConfigOption {
    fn default() -> Self {
        ConfigOption {
            base_dir: default_base_dir(),
            index_max_bytes: default_index_max_bytes(),
            index_max_interval_bytes: default_index_max_interval_bytes(),
            segment_max_bytes: default_segment_max_bytes(),
            flush_write_count: default_flush_write_count(),
        }
    }
}
