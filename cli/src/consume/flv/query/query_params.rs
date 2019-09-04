//!
//! # Fluvio -- Topic/Partition Parameters
//!
//! Intermediate structure to collect metadata information
//!
use std::net::SocketAddr;

use kf_protocol::api::Offset;

/// Fetch Logs parameters
#[derive(Debug)]
pub struct FlvFetchLogsParam {
    pub topic: String,
    pub max_bytes: i32,

    pub partitions: Vec<FlvPartitionParam>,
}

/// Topic/Partition parameters
#[derive(Debug, Clone, PartialEq)]
pub struct FlvTopicPartitionParam {
    pub topic_name: String,

    pub leaders: Vec<FlvLeaderParam>,
}

/// Replica Leader parameters
#[derive(Debug, Clone, PartialEq)]
pub struct FlvLeaderParam {
    pub leader_id: i32,
    pub server_addr: SocketAddr,

    pub partitions: Vec<FlvPartitionParam>,
}

/// Partition parameters
#[derive(Debug, Clone, PartialEq)]
pub struct FlvPartitionParam {
    pub partition_idx: i32,
    pub offset: Offset,
}
