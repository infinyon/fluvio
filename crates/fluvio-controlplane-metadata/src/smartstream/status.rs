//!
//! # SmartStream Status
//!
use std::fmt;

use dataplane::core::{Encoder, Decoder};
// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Default, Decoder, Encoder, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamStatus;

impl fmt::Display for SmartStreamStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartStreamStatus")
    }
}
