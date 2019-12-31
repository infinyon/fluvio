//!
//! # Streaming Coordinator Metadata
//!
//! Metadata stores a copy of the data from KV store in local memory.
//!
use std::sync::Arc;

use crate::config::ScConfig;
use crate::core::partitions::PartitionLocalStore;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::spus::SpuLocalStore;
use crate::core::topics::TopicLocalStore;

pub type ShareLocalStores = Arc<LocalStores>;

#[derive(Debug)]
pub struct LocalStores {
    spus: SharedSpuLocalStore,
    partitions: Arc<PartitionLocalStore>,
    topics: Arc<TopicLocalStore>,
    config: ScConfig,

}

// -----------------------------------
// ScMetadata - Implementation
// -----------------------------------

impl LocalStores {
    pub fn shared_metadata(config: ScConfig) -> Arc<Self> {
        Arc::new(LocalStores::new(config))
    }

    /// private function to provision metadata
    fn new(config: ScConfig) -> Self {
        LocalStores {
            spus: SpuLocalStore::new_shared(),
            partitions: PartitionLocalStore::new_shared(),
            topics: TopicLocalStore::new_shared(),
            config: config,
        }
    }


    /// reference to spus
    pub fn spus(&self) -> &SharedSpuLocalStore {
        &self.spus
    }

    pub fn owned_spus(&self) -> SharedSpuLocalStore {
        self.spus().clone()
    }

    /// reference to partitions
    pub fn partitions(&self) -> &Arc<PartitionLocalStore> {
        &self.partitions
    }

    /// reference to topics
    pub fn topics(&self) -> &Arc<TopicLocalStore> {
        &self.topics
    }

    /// reference to config
    pub fn config(&self) -> &ScConfig {
        &self.config
    }

    /// format metadata cache into a table string
    #[allow(dead_code)]
    pub fn table_fmt(&self) -> String {
        let mut table = String::new();
        let newline = format!("\n");

        table.push_str(&self.spus.table_fmt());
        table.push_str(&newline);
        table.push_str(&self.topics.table_fmt());
        table.push_str(&newline);
        table.push_str(&self.partitions.table_fmt());
        table
    }
}
