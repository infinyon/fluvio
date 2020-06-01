//!
//! # Fetch Topics
//!
//! Public API to retrieve Topics from the SC.
//!
use kf_protocol::api::Request;
use kf_protocol::api::FlvErrorCode;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

use flv_metadata::topic::{TopicSpec, TopicStatus};

use crate::ScApiKey;

// -----------------------------------
// FlvFetchTopicsRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchTopicsRequest {
    /// A list of one or more topics to be retrieved.
    /// None retrieves all topics.
    pub names: Option<Vec<String>>,
}

// -----------------------------------
// FlvFetchTopicsResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchTopicsResponse {
    /// The list of topics that have been retrieved.
    pub topics: Vec<FlvFetchTopicResponse>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchTopicResponse {
    /// The error code, None for no errors
    pub error_code: FlvErrorCode,

    /// The name of the topic.
    pub name: String,

    /// Topic parameters, None if error
    pub topic: Option<FlvFetchTopic>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchTopic {
    /// Topic spec
    pub spec: TopicSpec,

    /// Topic status
    pub status: TopicStatus,

    /// Replica assignment for each partition
    pub partition_replicas: Option<Vec<FlvPartitionReplica>>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvPartitionReplica {
    /// Partition id
    pub id: i32,

    /// Replica leader
    pub leader: i32,

    /// Replica assignment
    pub replicas: Vec<i32>,

    /// Only live replicas in replica assignment
    pub live_replicas: Vec<i32>,
}

// -----------------------------------
// Implementation - FlvFetchTopicsRequest
// -----------------------------------

impl Request for FlvFetchTopicsRequest {
    const API_KEY: u16 = ScApiKey::FlvFetchTopics as u16;
    type Response = FlvFetchTopicsResponse;
}

// -----------------------------------
// Implementation - FlvFetchTopicResponse
// -----------------------------------
impl FlvFetchTopicResponse {
    /// Constructor for topics found
    pub fn new(
        name: String,
        spec: TopicSpec,
        status: TopicStatus,
        partition_replicas: Option<Vec<FlvPartitionReplica>>,
    ) -> Self {
        FlvFetchTopicResponse {
            name: name,
            error_code: FlvErrorCode::None,
            topic: Some(FlvFetchTopic {
                spec,
                status,
                partition_replicas,
            }),
        }
    }

    /// Constructor for topics that are not found
    pub fn new_not_found(name: String) -> Self {
        FlvFetchTopicResponse {
            name: name,
            error_code: FlvErrorCode::TopicNotFound,
            topic: None,
        }
    }

    /// Update topic partitions.
    /// Requirements:
    ///  * Must be called with valid topic, otherwise, update will fail silently
    pub fn update_partitions(&mut self, partition_replicas: Option<Vec<FlvPartitionReplica>>) {
        if self.topic.is_some() {
            self.topic.as_mut().unwrap().partition_replicas = partition_replicas;
        }
    }
}
