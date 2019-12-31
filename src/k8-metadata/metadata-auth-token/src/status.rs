//!
//! #  AuthToken Status
//!
//! Interface to the AuthToken metadata status in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use metadata_core::Status;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AuthTokenStatus {
    pub resolution: TokenResolution,
    pub reason: String,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum TokenResolution {
    Ok,      // operational
    Init,    // initializing
    Invalid, // inactive
}

impl Status for AuthTokenStatus {}
