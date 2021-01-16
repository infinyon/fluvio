//!
//! # Streaming Processing Unit Configurations
//!
//! Stores configuration parameter used by Streaming Processing Unit module.
//! Parameters looked-up in following sequence (first value wins):
//!     1) cli parameters
//!     2) environment variables
//!     3) custom configuration or default configuration (from file)
//!

use std::env;
use std::path::PathBuf;

// defaults values
use fluvio_types::defaults::SPU_PUBLIC_PORT;
use fluvio_types::defaults::SPU_PRIVATE_PORT;
use fluvio_types::defaults::SC_PRIVATE_PORT;
use fluvio_types::defaults::SPU_LOG_BASE_DIR;
use fluvio_types::defaults::SPU_LOG_SIZE;
use fluvio_types::defaults::SPU_LOG_INDEX_MAX_BYTES;
use fluvio_types::defaults::SPU_LOG_INDEX_MAX_INTERVAL_BYTES;
use fluvio_types::defaults::SPU_LOG_SEGMENT_MAX_BYTES;
use fluvio_types::defaults::SPU_RETRY_SC_TIMEOUT_MS;

// environment variables

use fluvio_types::defaults::SPU_MIN_IN_SYNC_REPLICAS;
use fluvio_types::defaults::FLV_LOG_BASE_DIR;
use fluvio_types::defaults::FLV_LOG_SIZE;
use fluvio_types::SpuId;
use fluvio_storage::ConfigOption;
use fluvio_storage::DEFAULT_FLUSH_WRITE_COUNT;
use fluvio_storage::DEFAULT_FLUSH_IDLE_MSEC;

#[derive(Debug, PartialEq, Clone)]
pub struct Replication {
    pub min_in_sync_replicas: u16,
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            min_in_sync_replicas: SPU_MIN_IN_SYNC_REPLICAS,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Log {
    pub base_dir: PathBuf,
    pub size: String,
    pub index_max_bytes: u32,
    pub index_max_interval_bytes: u32,
    pub segment_max_bytes: u32,
    pub flush_write_count: u32,
    pub flush_idle_msec: u32,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from(
                env::var(FLV_LOG_BASE_DIR).unwrap_or_else(|_| SPU_LOG_BASE_DIR.to_owned()),
            ),
            size: env::var(FLV_LOG_SIZE).unwrap_or_else(|_| SPU_LOG_SIZE.to_owned()),
            index_max_bytes: SPU_LOG_INDEX_MAX_BYTES,
            index_max_interval_bytes: SPU_LOG_INDEX_MAX_INTERVAL_BYTES,
            segment_max_bytes: SPU_LOG_SEGMENT_MAX_BYTES,
            flush_write_count: DEFAULT_FLUSH_WRITE_COUNT,
            flush_idle_msec: DEFAULT_FLUSH_IDLE_MSEC,
        }
    }
}

impl Log {
    /// create new storage config
    pub fn new_config(&self) -> ConfigOption {
        ConfigOption::new(
            self.base_dir.clone(),
            self.index_max_bytes,
            self.index_max_interval_bytes,
            self.segment_max_bytes,
            self.flush_write_count,
            self.flush_idle_msec,
        )
    }
}

/// streaming processing unit configuration file
#[derive(Debug, PartialEq, Clone)]
pub struct SpuConfig {
    pub id: SpuId,

    pub rack: Option<String>,

    // spu (local server) points
    pub public_endpoint: String,
    pub private_endpoint: String,

    // sc (remote server) endpoint
    pub sc_endpoint: String,
    pub sc_retry_ms: u16,

    // parameters
    pub replication: Replication,
    pub log: Log,

    pub peer_max_bytes: u32,
}

impl Default for SpuConfig {
    fn default() -> Self {
        Self {
            id: 0,
            rack: None,
            public_endpoint: format!("0.0.0.0:{}", SPU_PUBLIC_PORT),
            private_endpoint: format!("0.0.0.0:{}", SPU_PRIVATE_PORT),
            sc_endpoint: format!("localhost:{}", SC_PRIVATE_PORT),
            replication: Replication::default(),
            sc_retry_ms: SPU_RETRY_SC_TIMEOUT_MS,
            log: Log::default(),
            peer_max_bytes: fluvio_storage::FileReplica::PREFER_MAX_LEN,
        }
    }
}

impl SpuConfig {
    pub fn id(&self) -> SpuId {
        self.id
    }

    #[allow(unused)]
    pub fn rack(&self) -> &Option<String> {
        &self.rack
    }

    pub fn sc_endpoint(&self) -> &str {
        &self.sc_endpoint
    }

    pub fn public_socket_addr(&self) -> &str {
        &self.public_endpoint
    }

    #[allow(unused)]
    pub fn public_server_addr(&self) -> &str {
        &self.public_endpoint
    }

    #[allow(unused)]
    pub fn private_socket_addr(&self) -> &str {
        &self.private_endpoint
    }

    pub fn storage(&self) -> &Log {
        &self.log
    }
}
