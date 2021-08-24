#![allow(clippy::assign_op_pattern)]

use std::fmt;

use dataplane::core::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorStatus {
    /// Status resolution
    pub resolution: ManagedConnectorStatusResolution,

    /// Reason for Status resolution (if applies)
    pub reason: Option<String>,
}

impl fmt::Display for ManagedConnectorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl ManagedConnectorStatus {
    pub fn invalid(reason: String) -> Self {
        Self {
            resolution: ManagedConnectorStatusResolution::Invalid,
            reason: Some(reason),
        }
    }

    pub fn reserved() -> Self {
        Self {
            resolution: ManagedConnectorStatusResolution::Reserved,
            ..Default::default()
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.resolution == ManagedConnectorStatusResolution::Reserved
    }
}

#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Encoder, Decoder, Debug, Clone, PartialEq)]
pub enum ManagedConnectorStatusResolution {
    Init,
    Invalid,
    Reserved,
}

// -----------------------------------
// Implementation - FlvSpuGroupResolution
// -----------------------------------
impl Default for ManagedConnectorStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}

impl fmt::Display for ManagedConnectorStatusResolution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Init => write!(f, "Init"),
            Self::Invalid => write!(f, "Invalid"),
            Self::Reserved => write!(f, "Reserved"),
        }
    }
}
