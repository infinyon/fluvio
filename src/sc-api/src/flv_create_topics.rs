//!
//! # Create Topics
//!
//! Public API to request the SC to create one or more topics.
//!
//!

use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use flv_metadata::topic::TopicSpec as TopicConfigMetadata;

use crate::FlvResponseMessage;
use crate::ScApiKey;

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateTopicsRequest {
    /// A list of one or more topics to be created.
    pub topics: Vec<FlvCreateTopicRequest>,

    /// Validate-only flag to prevent topic generation. Method is particularly useful
    /// to validate custom replicas.
    pub validate_only: bool,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateTopicRequest {
    /// The name of the topic.
    pub name: String,

    /// The Topic Metadata
    pub topic: TopicConfigMetadata,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateTopicsResponse {
    /// The topic creation result messages.
    pub results: Vec<FlvResponseMessage>,
}

impl Request for FlvCreateTopicsRequest {
    const API_KEY: u16 = ScApiKey::FlvCreateTopics as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvCreateTopicsResponse;
}
