//!
//! # Partition Status
//!
//! Interface to the Partition metadata status in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use k8_obj_metadata::Status;

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStatus {
    pub resolution: PartitionResolution,
    pub leader: ReplicaStatus,
    pub replicas: Vec<ReplicaStatus>,
    pub lsr: u32,
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    pub spu: i32,
    pub hw: i64,
    pub leo: i64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum PartitionResolution {
    Offline, // no leader
    Online,  // leader is available
    LeaderOffline,
    ElectionLeaderFound,
}

impl Default for PartitionResolution {
    fn default() -> Self {
        PartitionResolution::Offline
    }
}

impl Status for PartitionStatus {}
