//!
//! # Kafka -- Query Topics
//!
//! Retrieves Kafka Metadata and convert Topics and to KfTopicMetadata
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use serde::Serialize;

use kf_protocol::message::metadata::MetadataResponseTopic;
use kf_protocol::message::metadata::MetadataResponsePartition;
use kf_protocol::message::metadata::KfMetadataResponse;
use kf_protocol::api::ErrorCode as KfErrorCode;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::query_kf_metadata;
use crate::common::kf_get_api_versions;

// -----------------------------------
// Data Structures (Serializable)
// -----------------------------------

#[derive(Serialize, Debug)]
pub struct KfTopicMetadata {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<KfErrorCode>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<Topic>,
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

// -----------------------------------
// Implementation
// -----------------------------------
impl KfTopicMetadata {
    pub fn new(response_topic: &MetadataResponseTopic) -> Self {
        // if error is present, convert it
        let error = if response_topic.error_code.is_error() {
            Some(response_topic.error_code)
        } else {
            None
        };

        // convert topic
        let topic = if error.is_none() {
            Some(Topic::new(&response_topic))
        } else {
            None
        };

        // build topic metadata
        KfTopicMetadata {
            name: response_topic.name.clone(),
            error: error,
            topic: topic,
        }
    }
}

impl Topic {
    pub fn new(response_topic: &MetadataResponseTopic) -> Self {
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

// -----------------------------------
//  Process Request
// -----------------------------------

/// Query Kafka server for Topics and convert to Topic Metadata
pub fn query_kf_topic_metadata(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<Vec<KfTopicMetadata>, CliError> {
    match run_block_on(get_version_and_query_metadata(server_addr, names)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("cannot retrieve metadata: {}", err),
        ))),
        Ok(metadata) => {
            let mut topic_metadata_list: Vec<KfTopicMetadata> = vec![];
            for topic in &metadata.topics {
                topic_metadata_list.push(KfTopicMetadata::new(topic));
            }
            Ok(topic_metadata_list)
        }
    }
}

// Connect to Kafka Controller, get version and query metadata
async fn get_version_and_query_metadata(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<KfMetadataResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let versions = kf_get_api_versions(&mut conn).await?;

    query_kf_metadata(&mut conn, names, &versions).await
}
