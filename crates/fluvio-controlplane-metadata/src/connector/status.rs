#![allow(clippy::assign_op_pattern)]

use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
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
            resolution: ManagedConnectorStatusResolution::Running,
            ..Default::default()
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.resolution == ManagedConnectorStatusResolution::Running
    }
}

#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq)]
pub enum ManagedConnectorStatusResolution {
    Init,
    Invalid,
    Running,
    Pending,
    Failed,
}

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
            Self::Running => write!(f, "Running"),
            Self::Pending => write!(f, "Pending"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}
