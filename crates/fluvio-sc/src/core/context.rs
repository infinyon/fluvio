//!
//! # Streaming Coordinator Metadata
//!
//! Metadata stores a copy of the data from KV store in local memory.
//!
use std::sync::Arc;

use fluvio_sc_schema::mirror::MirrorSpec;
use fluvio_stream_model::core::MetadataItem;

use crate::config::ScConfig;
use crate::stores::spu::*;
use crate::stores::partition::*;
use crate::stores::topic::*;
use crate::stores::spg::*;
use crate::stores::smartmodule::*;
use crate::stores::tableformat::*;
use crate::stores::*;

pub type SharedContext<C> = Arc<Context<C>>;
pub type K8SharedContext = Arc<Context<K8MetaItem>>;

/// Global Context for SC
/// This is where we store globally accessible data
#[derive(Debug)]
pub struct Context<C: MetadataItem> {
    spus: StoreContext<SpuSpec, C>,
    partitions: StoreContext<PartitionSpec, C>,
    topics: StoreContext<TopicSpec, C>,
    spgs: StoreContext<SpuGroupSpec, C>,
    smartmodules: StoreContext<SmartModuleSpec, C>,
    tableformats: StoreContext<TableFormatSpec, C>,
    mirrors: StoreContext<MirrorSpec, C>,
    health: SharedHealthCheck,
    config: ScConfig,
}

// -----------------------------------
// ScMetadata - Implementation
// -----------------------------------

impl<C: MetadataItem> Context<C> {
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
            smartmodules: StoreContext::new(),
            tableformats: StoreContext::new(),
            mirrors: StoreContext::new(),
            health: HealthCheck::shared(),
            config,
        }
    }

    /// reference to spus
    pub fn spus(&self) -> &StoreContext<SpuSpec, C> {
        &self.spus
    }

    /// reference to partitions
    pub fn partitions(&self) -> &StoreContext<PartitionSpec, C> {
        &self.partitions
    }

    /// reference to topics
    pub fn topics(&self) -> &StoreContext<TopicSpec, C> {
        &self.topics
    }

    pub fn spgs(&self) -> &StoreContext<SpuGroupSpec, C> {
        &self.spgs
    }

    pub fn smartmodules(&self) -> &StoreContext<SmartModuleSpec, C> {
        &self.smartmodules
    }

    pub fn tableformats(&self) -> &StoreContext<TableFormatSpec, C> {
        &self.tableformats
    }

    pub fn mirrors(&self) -> &StoreContext<MirrorSpec, C> {
        &self.mirrors
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
