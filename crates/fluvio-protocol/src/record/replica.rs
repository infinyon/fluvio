use std::convert::TryFrom;
use std::fmt;

use fluvio_types::PartitionId;

use crate::derive::{Encoder, Decoder};

#[derive(Hash, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Encoder, Decoder)]
pub struct ReplicaKey {
    pub topic: String,
    pub partition: PartitionId,
}

impl fmt::Debug for ReplicaKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({},{})", self.topic, self.partition)
    }
}

unsafe impl Send for ReplicaKey {}

unsafe impl Sync for ReplicaKey {}

impl ReplicaKey {
    pub fn new<S, P>(topic: S, partition: P) -> Self
    where
        S: Into<String>,
        P: Into<PartitionId>,
    {
        ReplicaKey {
            topic: topic.into(),
            partition: partition.into(),
        }
    }

    pub fn split(self) -> (String, PartitionId) {
        (self.topic, self.partition)
    }
}

impl std::fmt::Display for ReplicaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl<S> From<(S, PartitionId)> for ReplicaKey
where
    S: Into<String>,
{
    fn from(key: (S, PartitionId)) -> ReplicaKey {
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

#[derive(Debug)]
pub enum PartitionError {
    InvalidSyntax(String),
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSyntax(msg) => write!(f, "invalid partition syntax: {msg}"),
        }
    }
}

/// Offset information about Partition
pub trait PartitionOffset {
    /// last offset that was committed
    fn last_stable_offset(&self) -> i64;

    // beginning offset for the partition
    fn start_offset(&self) -> i64;
}

// returns a tuple (topic_name, idx)
pub fn decompose_partition_name(
    partition_name: &str,
) -> Result<(String, PartitionId), PartitionError> {
    let dash_pos = partition_name.rfind('-');
    if dash_pos.is_none() {
        return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
    }

    let pos = dash_pos.unwrap();
    if (pos + 1) >= partition_name.len() {
        return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
    }

    let topic_name = &partition_name[..pos];
    let idx_string = &partition_name[(pos + 1)..];
    let idx = match idx_string.parse::<PartitionId>() {
        Ok(n) => n,
        Err(_) => {
            return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
        }
    };

    Ok((topic_name.to_string(), idx))
}

pub fn create_partition_name(topic_name: &str, idx: &i32) -> String {
    format!("{topic_name}-{idx}")
}
