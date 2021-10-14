#![allow(clippy::assign_op_pattern)]

use std::fmt;

use dataplane::core::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableStatus {
    /// Status resolution
    pub resolution: TableStatusResolution,

    /// Reason for Status resolution (if applies)
    pub reason: Option<String>,
}

impl fmt::Display for TableStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl TableStatus {
    pub fn invalid(reason: String) -> Self {
        Self {
            resolution: TableStatusResolution::Invalid,
            reason: Some(reason),
        }
    }

    pub fn reserved() -> Self {
        Self {
            resolution: TableStatusResolution::Running,
            ..Default::default()
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.resolution == TableStatusResolution::Running
    }
}

#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Encoder, Decoder, Debug, Clone, PartialEq)]
pub enum TableStatusResolution {
    Init,
    Invalid,
    Running,
    Pending,
    Failed,
}

impl Default for TableStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}

impl fmt::Display for TableStatusResolution {
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
