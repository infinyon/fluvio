//!
//! # Fetch Topic Offsets
//!
//! API that allows CLI to fetch topic offsets.
//!
use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

use crate::SpuApiKey;
use crate::errors::FlvErrorCode;

// -----------------------------------
// FlvFetchOffsetsRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchOffsetsRequest {
    /// Each topic in the request.
    pub topics: Vec<FetchOffsetTopic>,
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

// -----------------------------------
// Implementation - KfListOffsetRequest
// -----------------------------------

impl Request for FlvFetchOffsetsRequest {
    const API_KEY: u16 = SpuApiKey::FlvFetchOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = FlvFetchOffsetsResponse;
}
