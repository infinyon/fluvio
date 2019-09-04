//!
//! # SPU Status
//!
//! Interface to the SPU metadata status in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use metadata_core::Status;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Deserialize, Serialize, Debug, PartialEq, Default,Clone)]
pub struct SpuStatus {
    pub resolution: SpuStatusResolution,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum SpuStatusResolution {
    Online,
    Offline,
    Init
}

impl Default for SpuStatusResolution {
    fn default() -> Self {
        SpuStatusResolution::Init
    }
}

impl Status for SpuStatus{}