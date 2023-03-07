#![allow(clippy::assign_op_pattern)]

use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TableFormatStatus {
    /// Status resolution
    pub resolution: TableFormatStatusResolution,

    /// Reason for Status resolution (if applies)
    pub reason: Option<String>,
}

impl fmt::Display for TableFormatStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl TableFormatStatus {
    pub fn invalid(reason: String) -> Self {
        Self {
            resolution: TableFormatStatusResolution::Invalid,
            reason: Some(reason),
        }
    }

    pub fn reserved() -> Self {
        Self {
            resolution: TableFormatStatusResolution::Running,
            ..Default::default()
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.resolution == TableFormatStatusResolution::Running
    }
}

#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq)]
pub enum TableFormatStatusResolution {
    #[fluvio(tag = 0)]
    Init,
    #[fluvio(tag = 1)]
    Invalid,
    #[fluvio(tag = 2)]
    Running,
    #[fluvio(tag = 3)]
    Pending,
    #[fluvio(tag = 4)]
    Failed,
}

impl Default for TableFormatStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}

impl fmt::Display for TableFormatStatusResolution {
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
