//!
//! # Topic Status
//!
//! Topic Status metadata information cached locally.
//!
use std::collections::BTreeMap;
use std::fmt;

use kf_protocol::derive::{Decode, Encode};


use flv_types::{ReplicaMap, SpuId};

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize,serde::Deserialize),serde(rename_all = "camelCase"))]
pub struct TopicStatus {
    pub resolution: TopicResolution,
    pub replica_map: BTreeMap<i32, Vec<i32>>,
    pub reason: String,
}

impl fmt::Display for TopicStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize,serde::Deserialize))]
pub enum TopicResolution {
    Init,                  // initializing this is starting state.
    Pending,               // Has valid config, ready for replica mapping assignment
    InsufficientResources, // replica map cannot be created due to lack of capacity
    InvalidConfig,         // invalid configuration
    Provisioned,           // topics are allocated
}

impl TopicResolution {
    pub fn resolution_label(&self) -> &'static str {
        match self {
            TopicResolution::Provisioned => "provisioned",
            TopicResolution::Init => "initializing",
            TopicResolution::Pending => "pending",
            TopicResolution::InsufficientResources => "insufficient-resources",
            TopicResolution::InvalidConfig => "invalid-config",
        }
    }

    pub fn is_invalid(&self) -> bool {
        match self {
            Self::InvalidConfig => true,
            _ => false,
        }
    }

    pub fn no_resource(&self) -> bool {
        match self {
            Self::InsufficientResources => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for TopicResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "resolution::{}", self.resolution_label())
    }
}

// -----------------------------------
// Encode - from KV Topic Status
// -----------------------------------


/*
// -----------------------------------
// Encode/Decode - Internal API
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
        }
    }
}

impl ::std::default::Default for TopicResolution {
    fn default() -> Self {
        TopicResolution::Init
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

fn create_replica_map(rows: Vec<Vec<i32>>) -> BTreeMap<i32, Vec<i32>> {
    let mut map = BTreeMap::new();
    for (idx, row) in rows.iter().enumerate() {
        map.insert(idx as i32, row.clone());
    }
    map
}

impl TopicStatus {
    pub fn new<S>(resolution: TopicResolution, replica_map: Vec<Vec<i32>>, reason: S) -> Self
    where
        S: Into<String>,
    {
        TopicStatus {
            resolution: resolution,
            replica_map: create_replica_map(replica_map),
            reason: reason.into(),
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
            format!("{}", map_rows)
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

    pub fn next_resolution_provisioned() -> (TopicResolution, String) {
        (TopicResolution::Provisioned, "".to_owned())
    }

    /// set to pending mode which means it is waiting for spu resources to be allocated
    pub fn next_resolution_pending() -> (TopicResolution, String) {
        (TopicResolution::Pending, super::PENDING_REASON.to_owned())
    }

    pub fn next_resolution_invalid_config<S>(reason: S) -> (TopicResolution, String)
    where
        S: Into<String>,
    {
        (TopicResolution::InvalidConfig, reason.into())
    }

    pub fn set_resolution_no_resource<S>(reason: S) -> (TopicResolution, String)
    where
        S: Into<String>,
    {
        (TopicResolution::InsufficientResources, reason.into())
    }

    pub fn set_next_resolution(&mut self, next: (TopicResolution, String)) {
        let (resolution, reason) = next;
        self.resolution = resolution;
        self.reason = reason;
    }
}
