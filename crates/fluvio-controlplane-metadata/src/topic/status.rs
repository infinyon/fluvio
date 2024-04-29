#![allow(clippy::assign_op_pattern)]

//!
//! # Topic Status
//!
//! Topic Status metadata information cached locally.
//!
use std::collections::BTreeMap;
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_types::{ReplicaMap, SpuId, PartitionId};

use crate::partition::PartitionMirrorConfig;

pub type MirrorMap = BTreeMap<PartitionId, PartitionMirrorConfig>;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicStatus {
    pub resolution: TopicResolution,
    pub replica_map: ReplicaMap,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 14)]
    pub mirror_map: MirrorMap,
    pub reason: String,
}

impl fmt::Display for TopicStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TopicResolution {
    #[default]
    #[fluvio(tag = 0)]
    Init, // Initializing this is starting state.
    #[fluvio(tag = 1)]
    Pending, // Has valid config, ready for replica mapping assignment
    #[fluvio(tag = 2)]
    InsufficientResources, // Replica map cannot be created due to lack of capacity
    #[fluvio(tag = 3)]
    InvalidConfig, // Invalid configuration
    #[fluvio(tag = 4)]
    Provisioned, // All partitions has been provisioned
    #[fluvio(tag = 5)]
    Deleting, // Process of being deleted
}

impl TopicResolution {
    pub fn resolution_label(&self) -> &'static str {
        match self {
            TopicResolution::Provisioned => "provisioned",
            TopicResolution::Init => "initializing",
            TopicResolution::Pending => "pending",
            TopicResolution::InsufficientResources => "insufficient-resources",
            TopicResolution::InvalidConfig => "invalid-config",
            TopicResolution::Deleting => "Deleting",
        }
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::InvalidConfig)
    }

    pub fn no_resource(&self) -> bool {
        matches!(self, Self::InsufficientResources)
    }

    pub fn is_being_deleted(&self) -> bool {
        matches!(self, Self::Deleting)
    }
}

impl std::fmt::Display for TopicResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "resolution::{}", self.resolution_label())
    }
}

// -----------------------------------
// Encoder - from KV Topic Status
// -----------------------------------

/*
// -----------------------------------
// Encoder/Decoder - Internal API
// -----------------------------------

impl From<&TopicResolution> for u8 {
    fn from(spec: &TopicResolution) -> Self {
        match spec {
            TopicResolution::Init => 0 as u8,
            TopicResolution::Pending => 1 as u8,
            TopicResolution::NoResourceForReplicaMap => 2 as u8,
            TopicResolution::InvalidConfig => 3 as u8,
            TopicResolution::Provisioned => 4 as u8,
        }
    }
}
*/

// -----------------------------------
// Default
// -----------------------------------

impl ::std::default::Default for TopicStatus {
    fn default() -> Self {
        TopicStatus {
            resolution: TopicResolution::Init,
            replica_map: BTreeMap::new(),
            reason: "".to_owned(),
            mirror_map: BTreeMap::new(),
        }
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

fn create_replica_map(rows: Vec<Vec<SpuId>>) -> ReplicaMap {
    let mut map: ReplicaMap = BTreeMap::new();
    for (idx, row) in rows.iter().enumerate() {
        map.insert(idx as PartitionId, row.clone());
    }
    map
}

impl TopicStatus {
    pub fn new(
        resolution: TopicResolution,
        replica_map: Vec<Vec<SpuId>>,
        reason: impl Into<String>,
    ) -> Self {
        TopicStatus {
            resolution,
            replica_map: create_replica_map(replica_map),
            reason: reason.into(),
            mirror_map: BTreeMap::new(),
        }
    }

    pub fn resolution(&self) -> &TopicResolution {
        &self.resolution
    }

    pub fn replica_map_cnt(&self) -> i32 {
        self.replica_map.len() as i32
    }

    pub fn set_replica_map(&mut self, replica_map: ReplicaMap) {
        self.replica_map = replica_map;
    }

    pub fn set_mirror_map(&mut self, mirror_map: MirrorMap) {
        self.mirror_map = mirror_map;
    }

    pub fn spus_in_replica(&self) -> Vec<SpuId> {
        let mut spu_list: Vec<SpuId> = vec![];

        for (_, replicas) in self.replica_map.iter() {
            for spu in replicas {
                if !spu_list.contains(spu) {
                    spu_list.push(*spu);
                }
            }
        }

        spu_list
    }

    pub fn replica_map_str(&self) -> String {
        format!("{:?}", self.replica_map)
    }

    pub fn replica_map_cnt_str(&self) -> String {
        let map_rows = self.replica_map_cnt();
        if map_rows > 0 {
            format!("{map_rows}")
        } else {
            "-".to_owned()
        }
    }

    pub fn reason_str(&self) -> &String {
        &self.reason
    }

    // -----------------------------------
    // State Machine
    // -----------------------------------

    pub fn is_resolution_initializing(&self) -> bool {
        self.resolution == TopicResolution::Init
    }

    /// need to update the replic map
    pub fn need_replica_map_recal(&self) -> bool {
        self.resolution == TopicResolution::Pending
            || self.resolution == TopicResolution::InsufficientResources
    }

    pub fn is_resolution_pending(&self) -> bool {
        self.resolution == TopicResolution::Pending
    }

    pub fn is_resolution_transient(&self) -> bool {
        self.resolution == TopicResolution::Init || self.resolution == TopicResolution::Pending
    }

    pub fn is_resolution_provisioned(&self) -> bool {
        self.resolution == TopicResolution::Provisioned
    }

    /// set to pending mode which means it is waiting for spu resources to be allocated
    pub fn next_resolution_pending() -> (TopicResolution, String) {
        (TopicResolution::Pending, super::PENDING_REASON.to_owned())
    }

    pub fn next_resolution_invalid_config(reason: impl Into<String>) -> (TopicResolution, String) {
        (TopicResolution::InvalidConfig, reason.into())
    }

    pub fn set_next_resolution(&mut self, next: (TopicResolution, String)) {
        let (resolution, reason) = next;
        self.resolution = resolution;
        self.reason = reason;
    }
}
