//!
//! # Topic Status
//!
//! Interface to the Topic metadata status in K8 key value store
//!
use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use k8_obj_metadata::Status;

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatus {
    pub resolution: TopicStatusResolution,
    pub replica_map: BTreeMap<i32, Vec<i32>>,
    pub reason: String,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum TopicStatusResolution {
    Init,                  // initial state
    Pending,               // waiting for resources (spus)
    InsufficientResources, // out of sync with partition/replication_factor
    InvalidConfig,         // invalid configuration
    Provisioned,           // spu allocated
}

impl Default for TopicStatusResolution {
    fn default() -> Self {
        TopicStatusResolution::Init
    }
}

impl Status for TopicStatus {}
