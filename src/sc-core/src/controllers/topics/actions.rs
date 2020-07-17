//!
//! # Topic Actions
//!
//! Topic actions define operations performed on the Topics.
//!
use std::fmt;

use crate::controllers::partitions::PartitionWSAction;

use super::*;


#[derive(Debug, Default)]
pub struct TopicActions {
    pub topics: Vec<TopicWSAction>,
    pub partitions: Vec<PartitionWSAction>,
}

impl fmt::Display for TopicActions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.topics.len() == 0 && self.partitions.len() == 0 {
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
