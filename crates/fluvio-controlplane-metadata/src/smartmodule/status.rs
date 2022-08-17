//!
//! # SmartModule Status
//!
//! SmartModule Status metadata information cached locally.
//!
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Default, Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartModuleStatus;

impl fmt::Display for SmartModuleStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleStatus")
    }
}
