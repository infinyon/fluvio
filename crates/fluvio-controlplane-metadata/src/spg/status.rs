#![allow(clippy::assign_op_pattern)]

use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SpuGroupStatus {
    /// Status resolution
    pub resolution: SpuGroupStatusResolution,

    /// Reason for Status resolution (if applies)
    pub reason: Option<String>,
}

impl fmt::Display for SpuGroupStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

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

#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq)]
pub enum SpuGroupStatusResolution {
    #[fluvio(tag = 0)]
    Init,
    #[fluvio(tag = 1)]
    Invalid,
    #[fluvio(tag = 2)]
    Reserved,
}

// -----------------------------------
// Implementation - FlvSpuGroupResolution
// -----------------------------------
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
