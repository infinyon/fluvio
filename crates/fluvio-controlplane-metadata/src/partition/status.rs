#![allow(clippy::assign_op_pattern)]

//!
//! # Partition Status
//!
//! Partition Status metadata information cached locally.
//!

use std::fmt;
use std::slice::Iter;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::record::Offset;
use fluvio_types::SpuId;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct PartitionStatus {
    pub resolution: PartitionResolution,
    pub leader: ReplicaStatus,
    // TODO: Next time we make a breaking protocol change, rename this to `lrs`
    // TODO: There is no such thing as `lsr`, it is a typo
    #[cfg_attr(feature = "use_serde", serde(alias = "lrs"))]
    pub lsr: u32,
    pub replicas: Vec<ReplicaStatus>,
    #[cfg_attr(
        feature = "use_serde",
        serde(default = "default_partition_status_size")
    )]
    #[fluvio(min_version = 5)]
    pub size: i64,
    pub is_being_deleted: bool,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 16)]
    pub base_offset: i64,
}

impl Default for PartitionStatus {
    fn default() -> Self {
        Self {
            size: PartitionStatus::SIZE_NOT_SUPPORTED,
            resolution: Default::default(),
            leader: Default::default(),
            lsr: Default::default(),
            replicas: Default::default(),
            is_being_deleted: Default::default(),
            base_offset: Default::default(),
        }
    }
}

#[cfg(feature = "use_serde")]
const fn default_partition_status_size() -> i64 {
    PartitionStatus::SIZE_NOT_SUPPORTED
}

impl fmt::Display for PartitionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?} Leader: {} [", self.resolution, self.leader)?;
        for replica in &self.replicas {
            write!(f, "{replica},")?;
        }
        write!(f, "]")
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

impl PartitionStatus {
    pub const SIZE_ERROR: i64 = -1;
    pub const SIZE_NOT_SUPPORTED: i64 = -2;

    pub fn leader(leader: impl Into<ReplicaStatus>) -> Self {
        Self::new(leader.into(), vec![])
    }

    pub fn new(leader: impl Into<ReplicaStatus>, replicas: Vec<ReplicaStatus>) -> Self {
        Self {
            resolution: PartitionResolution::default(),
            leader: leader.into(),
            replicas,
            ..Default::default()
        }
    }

    pub fn new2(
        leader: impl Into<ReplicaStatus>,
        replicas: Vec<ReplicaStatus>,
        size: i64,
        resolution: PartitionResolution,
        base_offset: i64,
    ) -> Self {
        Self {
            resolution,
            leader: leader.into(),
            replicas,
            size,
            base_offset,
            ..Default::default()
        }
    }

    pub fn is_online(&self) -> bool {
        self.resolution == PartitionResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution != PartitionResolution::Online
    }

    #[deprecated = "Replaced by lrs()"]
    pub fn lsr(&self) -> u32 {
        self.lsr
    }

    pub fn lrs(&self) -> u32 {
        self.lsr
    }

    pub fn replica_iter(&self) -> Iter<ReplicaStatus> {
        self.replicas.iter()
    }

    pub fn live_replicas(&self) -> Vec<SpuId> {
        self.replicas.iter().map(|lrs| lrs.spu).collect()
    }

    pub fn offline_replicas(&self) -> Vec<i32> {
        vec![]
    }

    pub fn has_live_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    /// set to being deleted
    pub fn set_to_delete(mut self) -> Self {
        self.is_being_deleted = true;
        self
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PartitionResolution {
    #[default]
    #[fluvio(tag = 0)]
    Offline, // No leader available for serving partition
    #[fluvio(tag = 1)]
    Online, // Partition is running normally, status contains replica info
    #[fluvio(tag = 2)]
    LeaderOffline, // Election has failed, no suitable leader has been found
    #[fluvio(tag = 3)]
    ElectionLeaderFound, // New leader has been selected
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ReplicaStatus {
    pub spu: SpuId,
    pub hw: i64,
    pub leo: i64,
}

impl fmt::Display for ReplicaStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "spu:{} hw:{} leo: {}", self.spu, self.hw, self.leo)
    }
}

impl Default for ReplicaStatus {
    fn default() -> Self {
        ReplicaStatus {
            spu: -1,
            hw: -1,
            leo: -1,
        }
    }
}

impl ReplicaStatus {
    pub fn new(spu: SpuId, hw: Offset, leo: Offset) -> Self {
        Self { spu, hw, leo }
    }

    /// compute lag score respect to leader
    pub fn leader_lag(&self, leader_status: &Self) -> i64 {
        leader_status.leo - self.leo
    }

    pub fn high_watermark_lag(&self, leader_status: &Self) -> i64 {
        leader_status.hw - self.hw
    }
}

impl From<(SpuId, Offset, Offset)> for ReplicaStatus {
    fn from(val: (SpuId, Offset, Offset)) -> Self {
        let (id, high_watermark, end_offset) = val;
        Self::new(id, high_watermark, end_offset)
    }
}
