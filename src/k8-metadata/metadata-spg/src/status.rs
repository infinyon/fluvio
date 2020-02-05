//!
//! # Cluster Status
//!
//! Interface to the Cluster metadata status in K8 key value store
//!
use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use k8_obj_metadata::Status;

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuGroupStatus {
    pub resolution: SpuGroupStatusResolution,
    pub reason: Option<String>,
}

impl Status for SpuGroupStatus {}

impl SpuGroupStatus {
    pub fn invalid(reason: String) -> Self {
        Self {
            resolution: SpuGroupStatusResolution::Invalid,
            reason: Some(reason),
        }
    }

    pub fn reserved() -> Self {
        Self {
            resolution: SpuGroupStatusResolution::Reserved,
            ..Default::default()
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.resolution == SpuGroupStatusResolution::Reserved
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum SpuGroupStatusResolution {
    Init,
    Invalid,
    Reserved,
}

impl Default for SpuGroupStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}

impl fmt::Display for SpuGroupStatusResolution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Init => write!(f, "Init"),
            Self::Invalid => write!(f, "Invalid"),
            Self::Reserved => write!(f, "Reserved"),
        }
    }
}
