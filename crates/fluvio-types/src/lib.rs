use std::collections::BTreeMap;

pub mod compression;
pub mod defaults;
pub mod macros;
pub mod partition;
pub mod config_file;

#[cfg(feature = "events")]
pub mod event;

pub use partition::PartitionError;

//
// Types
//
pub type ReplicaMap = BTreeMap<PartitionId, Vec<SpuId>>;
pub type Reason = String;
pub type Name = String;

pub type SpuName = String;
pub type SpuId = i32;

pub type SmartModuleName = String;

pub type IsOnline = bool;
pub type IsOk = bool;

// Topic
pub type TopicName = String;
pub type PartitionId = u32;
pub type PartitionCount = u32;
pub type ReplicationFactor = u32;
pub type IgnoreRackAssignment = bool;

// AuthToken
pub type TokenName = String;
pub type TokenSecret = String;

// Time
pub type Timestamp = i64;
