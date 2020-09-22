//!
//! # Parameters used by users
//!
//!

use dataplane::Offset;

/// Fetch Logs parameters
#[derive(Debug)]
pub struct FetchLogsParam {
    pub topic: String,
    pub max_bytes: i32,

    pub partitions: Vec<PartitionParam>,
}

/// Topic/Partition parameters
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitionParam {
    pub topic_name: String,

    pub leaders: Vec<LeaderParam>,
}

/// Replica Leader parameters
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderParam {
    pub leader_id: i32,
    pub server_addr: String,

    pub partitions: Vec<PartitionParam>,
}

/// Partition parameters
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PartitionParam {
    pub partition_idx: i32,
    pub offset: Offset,
    pub epoch: i32,
}
