//!
//! # ReplicKey
//!
//! Replica Key is a composition of of Topic/Partition
//!
use std::convert::TryFrom;

use kf_protocol::derive::{Decode, Encode};

use types::PartitionError;
use types::partition::decompose_partition_name;

#[derive(Hash, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct ReplicaKey {
    pub topic: String,
    pub partition: i32,
}

impl ReplicaKey {
    pub fn new<S, P>(topic: S, partition: P) -> Self
    where
        S: Into<String>,
        P: Into<i32>,
    {
        ReplicaKey {
            topic: topic.into(),
            partition: partition.into(),
        }
    }

    pub fn split(self) -> (String,i32) {
        (self.topic,self.partition)
    }
}

impl std::fmt::Display for ReplicaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl<S> From<(S, i32)> for ReplicaKey
where
    S: Into<String>,
{
    fn from(key: (S, i32)) -> ReplicaKey {
        ReplicaKey::new(key.0.into(), key.1)
    }
}

impl TryFrom<String> for ReplicaKey {
    type Error = PartitionError;

    fn try_from(value: String) -> Result<Self, PartitionError> {
        let (topic, partition) = decompose_partition_name(&value)?;
        Ok(ReplicaKey::new(topic, partition))
    }
}
