//!
//! # Kafka -- Query Topics
//!
//! Retrieves Kafka Metadata and convert Topics and to KfTopicMetadata
//!

use serde::Serialize;

use kf_protocol::message::metadata::MetadataResponseTopic;
use kf_protocol::message::metadata::MetadataResponsePartition;
use kf_protocol::api::ErrorCode as KfErrorCode;

#[derive(Serialize, Debug)]
pub struct KfTopicMetadata {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<KfErrorCode>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<Topic>,
}

impl KfTopicMetadata {
    pub fn new(response_topic: MetadataResponseTopic) -> Self {
        let name = response_topic.name.clone();

        // if error is present, convert it
        let error = if response_topic.error_code.is_error() {
            Some(response_topic.error_code)
        } else {
            None
        };

        // convert topic
        let topic = if error.is_none() {
            Some(Topic::new(response_topic))
        } else {
            None
        };

        // build topic metadata
        KfTopicMetadata {
            name,
            error: error,
            topic: topic,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct Topic {
    pub is_internal: bool,
    pub partitions: i32,
    pub replication_factor: i32,
    pub partition_map: Vec<PartitionReplica>,
}

#[derive(Serialize, Debug)]
pub struct PartitionReplica {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,

    pub status: String,
}

impl Topic {
    pub fn new(response_topic: MetadataResponseTopic) -> Self {
        // convert partition replicas
        let mut partitions: Vec<PartitionReplica> = vec![];
        for response_partition in &response_topic.partitions {
            partitions.push(PartitionReplica::new(response_partition));
        }

        // compute partitions & replication factor
        let partition_cnt = response_topic.partitions.len() as i32;
        let replication_factor_cnt = if partition_cnt > 0 {
            response_topic.partitions[0].replica_nodes.len() as i32
        } else {
            0
        };

        Topic {
            is_internal: response_topic.is_internal,
            partitions: partition_cnt,
            replication_factor: replication_factor_cnt,
            partition_map: partitions,
        }
    }
}

impl PartitionReplica {
    pub fn new(response_partition: &MetadataResponsePartition) -> Self {
        PartitionReplica {
            id: response_partition.partition_index,
            leader: response_partition.leader_id,
            replicas: response_partition.replica_nodes.clone(),
            isr: response_partition.isr_nodes.clone(),
            status: response_partition.error_code.to_string(),
        }
    }
}
