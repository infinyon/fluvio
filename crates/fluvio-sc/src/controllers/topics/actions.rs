//!
//! # Topic Actions
//!
//! Topic actions define operations performed on the Topics.
//!
use std::fmt;

use fluvio_controlplane_metadata::{topic::TopicSpec, partition::PartitionSpec};
use fluvio_stream_model::{store::k8::K8MetaItem, core::MetadataItem};

use crate::stores::actions::WSAction;

#[derive(Debug, Default)]
pub struct TopicActions<C = K8MetaItem>
where
    C: MetadataItem,
{
    pub topics: Vec<WSAction<TopicSpec, C>>,
    pub partitions: Vec<WSAction<PartitionSpec, C>>,
}

impl fmt::Display for TopicActions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.topics.is_empty() && self.partitions.is_empty() {
            write!(f, "Empty topic actions")
        } else {
            write!(
                f,
                "Topic Actions: {}, Partition Actions:: {}, ",
                self.topics.len(),
                self.partitions.len(),
            )
        }
    }
}
