//!
//! # Fetch Topic Offsets
//!
//! API that allows CLI to fetch topic offsets.
//!
use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::PartitionOffset;

use crate::SpuApiKey;
use crate::errors::FlvErrorCode;

// -----------------------------------
// FlvFetchOffsetsRequest
// -----------------------------------

/// Fetch offsets
#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchOffsetsRequest {
    /// Each topic in the request.
    pub topics: Vec<FetchOffsetTopic>,
}

impl FlvFetchOffsetsRequest {

    /// create request with a single topic and partition
    pub fn new(topic: String,partition: i32) -> Self {
        Self {
            topics: vec![FetchOffsetTopic {
                name: topic,
                partitions: vec![FetchOffsetPartition {
                    partition_index: partition
                }]
            }]}
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct FetchOffsetTopic {
    /// The topic name.
    pub name: String,

    /// Each partition in the request.
    pub partitions: Vec<FetchOffsetPartition>,
}

#[derive(Decode, Encode, Default, Debug)]
pub struct FetchOffsetPartition {
    /// The partition index.
    pub partition_index: i32,
}

// -----------------------------------
// FlvFetchOffsetsResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchOffsetsResponse {
    /// Each topic offset in the response.
    pub topics: Vec<FetchOffsetTopicResponse>,
}

impl FlvFetchOffsetsResponse {

    pub fn find_partition(self,topic: &str,partition: i32) -> Option<FetchOffsetPartitionResponse> {

        for topic_res in self.topics {
            if topic_res.name == topic {
                for partition_res in topic_res.partitions {
                    if partition_res.partition_index == partition {
                        return Some(partition_res);
                    }
                }
            }
        }

        None

    }
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FetchOffsetTopicResponse {
    /// The topic name
    pub name: String,

    /// Each partition in the response.
    pub partitions: Vec<FetchOffsetPartitionResponse>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FetchOffsetPartitionResponse {
    /// The partition error code, None for no error
    pub error_code: FlvErrorCode,

    /// The partition index.
    pub partition_index: i32,

    /// First readable offset.
    pub start_offset: i64,

    /// Last readable offset
    pub last_stable_offset: i64,
}

impl PartitionOffset for FetchOffsetPartitionResponse {

    fn last_stable_offset(&self) -> i64 {
        self.last_stable_offset
    }

    fn start_offset(&self) -> i64 {
        self.start_offset
    }


}


// -----------------------------------
// Implementation - KfListOffsetRequest
// -----------------------------------

impl Request for FlvFetchOffsetsRequest {
    const API_KEY: u16 = SpuApiKey::FlvFetchOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = FlvFetchOffsetsResponse;
}
