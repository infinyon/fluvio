//!
//! # Streaming Coordinator Metadata
//!
//! Metadata stores a copy of the data from KV store in local memory.
//!
use std::sync::Arc;

use crate::config::ScConfig;
use crate::stores::spu::*;
use crate::stores::partition::*;
use crate::stores::topic::*;
use crate::stores::spg::*;
use crate::stores::managed_connector::*;
use crate::stores::*;

pub type SharedContext = Arc<Context>;

/// Global Context for SC
/// This is where we store globally accessible data
#[derive(Debug)]
pub struct Context {
    spus: StoreContext<SpuSpec>,
    partitions: StoreContext<PartitionSpec>,
    topics: StoreContext<TopicSpec>,
    spgs: StoreContext<SpuGroupSpec>,
    managed_connectors: StoreContext<ManagedConnectorSpec>,
    health: SharedHealthCheck,
    config: ScConfig,
}

// -----------------------------------
// ScMetadata - Implementation
// -----------------------------------

impl Context {
    pub fn shared_metadata(config: ScConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }

    /// private function to provision metadata
    fn new(config: ScConfig) -> Self {
        Self {
            spus: StoreContext::new(),
            partitions: StoreContext::new(),
            topics: StoreContext::new(),
            spgs: StoreContext::new(),
            managed_connectors: StoreContext::new(),
            health: HealthCheck::shared(),
            config,
        }
    }

    /// reference to spus
    pub fn spus(&self) -> &StoreContext<SpuSpec> {
        &self.spus
    }

    /// reference to partitions
    pub fn partitions(&self) -> &StoreContext<PartitionSpec> {
        &self.partitions
    }

    /// reference to topics
    pub fn topics(&self) -> &StoreContext<TopicSpec> {
        &self.topics
    }

    pub fn spgs(&self) -> &StoreContext<SpuGroupSpec> {
        &self.spgs
    }

    pub fn managed_connectors(&self) -> &StoreContext<ManagedConnectorSpec> {
        &self.managed_connectors
    }

    /// spu health channel
    pub fn health(&self) -> &SharedHealthCheck {
        &self.health
    }

    /// reference to config
    pub fn config(&self) -> &ScConfig {
        &self.config
    }

    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }
}
