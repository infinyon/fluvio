use std::convert::TryFrom;
use std::fmt;

use serde::{Serialize, Deserialize};

use kf_protocol_derive::Decode;
use kf_protocol_derive::Encode;

#[derive(Debug, Encode, Serialize, Deserialize, Decode, Clone)]
#[fluvio_kf(encode_discriminant)]
#[repr(u8)]
pub enum Isolation {
    ReadUncommitted = 0,
    ReadCommitted = 1,
}

impl Default for Isolation {
    fn default() -> Self {
        Isolation::ReadUncommitted
    }
}




#[derive(Hash, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct ReplicaKey {
    pub topic: String,
    pub partition: i32,
}

unsafe impl Send for ReplicaKey{}

unsafe impl Sync for ReplicaKey{}

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

    pub fn split(self) -> (String, i32) {
        (self.topic, self.partition)
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



#[derive(Debug)]
pub enum PartitionError {
    InvalidSyntax(String),
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSyntax(msg) => write!(f, "invalid partition syntax: {}", msg),
        }
    }
}

// returns a tuple (topic_name, idx)
pub fn decompose_partition_name(partition_name: &str) -> Result<(String, i32), PartitionError> {
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
    let idx = match idx_string.parse::<i32>() {
        Ok(n) => n,
        Err(_) => {
            return Err(PartitionError::InvalidSyntax(partition_name.to_owned()));
        }
    };

    Ok((topic_name.to_string(), idx))
}

pub fn create_partition_name(topic_name: &str, idx: &i32) -> String {
    format!("{}-{}", topic_name.clone(), idx)
}
