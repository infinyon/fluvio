//!
//! # Partition Map
//!
//! Partition map is read from file and converted to SC/KF configuration
//!

use serde::Deserialize;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs::read_to_string;
use std::path::Path;

use kf_protocol::message::topic::CreatableReplicaAssignment;
use sc_api::topic::FlvTopicPartitionMap;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Partitions {
    partitions: Vec<Partition>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Partition {
    id: i32,
    replicas: Vec<i32>,
}

// -----------------------------------
// Partitions - Decode/Encode
// -----------------------------------

impl Partitions {
    /// Read and decode the json file into Replica Assignment map
    pub fn file_decode<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
        let file_str: String = read_to_string(path)?;
        serde_json::from_str(&file_str)
            .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
    }

    // Encode Replica Assignment map into Kafka Create Replica Assignment
    pub fn kf_encode(&self) -> Vec<CreatableReplicaAssignment> {
        let mut assignments: Vec<CreatableReplicaAssignment> = vec![];
        for partition in &self.partitions {
            assignments.push(CreatableReplicaAssignment {
                partition_index: partition.id,
                broker_ids: partition.replicas.clone(),
            })
        }
        assignments
    }

    // Encode Replica Assignment map into Fluvio format
    pub fn sc_encode(&self) -> Vec<FlvTopicPartitionMap> {
        let mut partition_map: Vec<FlvTopicPartitionMap> = vec![];
        for partition in &self.partitions {
            partition_map.push(FlvTopicPartitionMap {
                id: partition.id,
                replicas: partition.replicas.clone(),
            })
        }
        partition_map
    }
}
