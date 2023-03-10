#![allow(clippy::assign_op_pattern)]

//!
//! # Spu Status
//!
//! Spu Status metadata information cached locally.
//!
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SpuStatus {
    pub resolution: SpuStatusResolution,
}

impl fmt::Display for SpuStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl SpuStatus {
    /// create offline status
    pub fn offline() -> Self {
        Self {
            resolution: SpuStatusResolution::Offline,
        }
    }
    /// Resolution to string label
    pub fn resolution_label(&self) -> &'static str {
        match self.resolution {
            SpuStatusResolution::Online => "online",
            SpuStatusResolution::Offline => "offline",
            SpuStatusResolution::Init => "Init",
        }
    }

    /// Checks if resolution is marked online. true for online, false otherwise
    pub fn is_online(&self) -> bool {
        self.resolution == SpuStatusResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution == SpuStatusResolution::Offline
    }

    pub fn is_init(&self) -> bool {
        self.resolution == SpuStatusResolution::Init
    }

    /// Set resolution to status to online
    pub fn set_online(&mut self) {
        self.resolution = SpuStatusResolution::Online;
    }

    /// Set resolution to status to offline
    pub fn set_offline(&mut self) {
        self.resolution = SpuStatusResolution::Offline;
    }
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SpuStatusResolution {
    #[fluvio(tag = 0)]
    Online,
    #[fluvio(tag = 1)]
    Offline,
    #[fluvio(tag = 2)]
    Init,
}

impl Default for SpuStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}
