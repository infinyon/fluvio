//!
//! # Topic Actions
//!
//! Topic actions define operations performed on the Topics.
//!
use std::fmt;

use flv_util::actions::Actions;

use crate::core::partitions::PartitionWSAction;
use crate::core::spus::SpuLSChange;

use super::TopicWSAction;
use super::TopicLSChange;

/// Change Request send to Topic
#[derive(Debug, PartialEq, Clone)]
pub enum TopicChangeRequest {
    Topic(Actions<TopicLSChange>),
    Spu(Actions<SpuLSChange>),
}

impl fmt::Display for TopicChangeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicChangeRequest::Topic(req) => write!(f, "Topic LS: {}", req.count()),
            TopicChangeRequest::Spu(req) => write!(f, "SPU LS: {}", req.count()),
        }
    }
}

#[derive(Debug, Default)]
pub struct TopicActions {
    pub topics: Actions<TopicWSAction>,
    pub partitions: Actions<PartitionWSAction>,
}

impl fmt::Display for TopicActions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.topics.count() == 0 && self.partitions.count() == 0 {
            write!(f, "Empty topic actions")
        } else {
            write!(
                f,
                "Topic Actions: {}, Partition Actions:: {}, ",
                self.topics.count(),
                self.partitions.count(),
            )
        }
    }
}
