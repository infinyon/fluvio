use std::collections::BTreeMap;

pub mod defaults;
pub mod macros;
pub mod partition;

#[cfg(feature = "events")]
pub mod event;

pub use partition::PartitionError;

//
// Types
//
pub type ReplicaMap = BTreeMap<i32, Vec<i32>>;
pub type Reason = String;
pub type Name = String;

pub type SpuName = String;
pub type SpuId = i32;

pub type SmartModuleName = String;

pub type IsOnline = bool;
pub type IsOk = bool;

// Topic
pub type TopicName = String;
pub type PartitionId = i32;
pub type PartitionCount = i32;
pub type ReplicationFactor = i32;
pub type IgnoreRackAssignment = bool;

// AuthToken
pub type TokenName = String;
pub type TokenSecret = String;

// Time
pub type Timestamp = i64;
