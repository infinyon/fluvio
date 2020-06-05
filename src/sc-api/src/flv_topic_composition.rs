//!
//! # Topic Composition
//!
//! API that allows CLI to fetch topic composition: Live Replicas and SPUs
//!
use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::FlvErrorCode;
use flv_types::SpuId;
use flv_util::socket_helpers::ServerAddress;

use crate::ScApiKey;

// -----------------------------------
// FlvTopicCompositionRequest
// -----------------------------------

/// Use id to fetch one entry, None to fetch all
#[derive(Decode, Encode, Default, Debug)]
pub struct FlvTopicCompositionRequest {
    pub topic_names: Vec<String>,
}

// -----------------------------------
// FlvTopicCompositionResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvTopicCompositionResponse {
    /// The topics requested
    pub topics: Vec<FetchTopicResponse>,

    /// The SPUs associated with replica assignment of the topics
    pub spus: Vec<FetchSpuResponse>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FetchTopicResponse {
    /// The error code, None for no errors
    pub error_code: FlvErrorCode,

    /// The topic name
    pub name: String,

    /// The partitions associated with the topic
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FetchPartitionResponse {
    /// The error code, None for no errors
    pub error_code: FlvErrorCode,

    /// The partition index.
    pub partition_idx: i32,

    /// The id of the leader SPU.
    pub leader_id: i32,

    /// The set of all spus that host this partition.
    pub replicas: Vec<i32>,

    /// The set of all live replica spus that host this partition.
    pub live_replicas: Vec<i32>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FetchSpuResponse {
    /// The error code, None for no errors
    pub error_code: FlvErrorCode,

    /// The spu ID.
    pub spu_id: SpuId,

    /// The spu public hostname.
    pub host: String,

    /// The spu public port.
    pub port: u16,
}

impl FetchSpuResponse {
    pub fn into(&self) -> ServerAddress {
        ServerAddress::new(self.host.clone(), self.port)
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

impl Request for FlvTopicCompositionRequest {
    const API_KEY: u16 = ScApiKey::FlvTopicComposition as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvTopicCompositionResponse;
}
