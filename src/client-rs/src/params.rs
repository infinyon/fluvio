//!
//! # Parameters used by users
//!
//!

use kf_protocol::api::*;

pub const MAX_FETCH_BYTES: u32 = 1000000;

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


pub enum FetchOffset {
    Earliest(Option<i64>),
    /// earliest + offset
    Latest(Option<i64>),
    /// latest - offset
    Offset(i64),
}

#[derive(Default)]
pub struct FetchLogOption {
    pub max_bytes: i32,
    pub isolation: Isolation,
}
