//!
//! # Fetch Topic Offsets
//!
//! API that allows CLI to fetch topic offsets.
use std::fmt;

use fluvio_protocol::api::Request;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::record::PartitionOffset;
use fluvio_protocol::record::ReplicaKey;

use fluvio_types::PartitionId;

use crate::errors::ErrorCode;
use super::SpuServerApiKey;

// -----------------------------------
// FlvFetchOffsetsRequest
// -----------------------------------

/// Fetch offsets
#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchOffsetsRequest {
    /// Each topic in the request.
    pub topics: Vec<FetchOffsetTopic>,
}

impl Request for FetchOffsetsRequest {
    const API_KEY: u16 = SpuServerApiKey::FetchOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = FetchOffsetsResponse;
}

impl FetchOffsetsRequest {
    /// create request with a single topic and partition
    pub fn new(topic: String, partition: u32) -> Self {
        Self {
            topics: vec![FetchOffsetTopic {
                name: topic,
                partitions: vec![FetchOffsetPartition {
                    partition_index: partition,
                }],
            }],
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchOffsetTopic {
    /// The topic name.
    pub name: String,

    /// Each partition in the request.
    pub partitions: Vec<FetchOffsetPartition>,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchOffsetPartition {
    /// The partition index.
    pub partition_index: PartitionId,
}

// -----------------------------------
// FlvFetchOffsetsResponse
// -----------------------------------

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchOffsetsResponse {
    /// Each topic offset in the response.
    pub topics: Vec<FetchOffsetTopicResponse>,
}

impl FetchOffsetsResponse {
    pub fn find_partition(self, replica: &ReplicaKey) -> Option<FetchOffsetPartitionResponse> {
        for topic_res in self.topics {
            if topic_res.name == replica.topic {
                for partition_res in topic_res.partitions {
                    if partition_res.partition_index == replica.partition {
                        return Some(partition_res);
                    }
                }
            }
        }

        None
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchOffsetTopicResponse {
    /// The topic name
    pub name: String,

    /// Each partition in the response.
    pub partitions: Vec<FetchOffsetPartitionResponse>,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchOffsetPartitionResponse {
    /// The partition error code, None for no error
    pub error_code: ErrorCode,

    /// The partition index.
    pub partition_index: PartitionId,

    /// First readable offset.
    pub start_offset: i64,

    /// Last readable offset
    pub last_stable_offset: i64,
}

impl fmt::Display for FetchOffsetPartitionResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "error: {:#?}, partition: {}, start: {}, last: {}",
            self.error_code, self.partition_index, self.start_offset, self.last_stable_offset
        )
    }
}

impl PartitionOffset for FetchOffsetPartitionResponse {
    fn last_stable_offset(&self) -> i64 {
        self.last_stable_offset
    }

    fn start_offset(&self) -> i64 {
        self.start_offset
    }
}
