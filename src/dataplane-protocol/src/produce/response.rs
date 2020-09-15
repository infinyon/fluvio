
use crate::derive::Encode;
use crate::derive::Decode;
use crate::derive::FluvioDefault;

use crate::ErrorCode;

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct ProduceResponse {
    /// Each produce response
    pub responses: Vec<TopicProduceResponse>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    #[fluvio(min_version = 1, ignorable)]
    pub throttle_time_ms: i32,
}

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct TopicProduceResponse {
    /// The topic name
    pub name: String,

    /// Each partition that we produced to within the topic.
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Encode, Decode,FluvioDefault, Debug)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub partition_index: i32,

    /// The error code, or 0 if there was no error.
    pub error_code: ErrorCode,

    /// The base offset.
    pub base_offset: i64,

    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the
    /// topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will
    /// be the broker local time when the messages are appended.
    #[fluvio(min_version = 2, ignorable)]
    pub log_append_time_ms: i64,

    /// The log start offset.
    #[fluvio(min_version = 5, ignorable)]
    pub log_start_offset: i64,
}
