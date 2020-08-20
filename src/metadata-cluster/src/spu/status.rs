#![allow(clippy::assign_op_pattern)]

//!
//! # Spu Status
//!
//! Spu Status metadata information cached locally.
//!
use std::fmt;

use kf_protocol::derive::{Decode, Encode};

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SpuStatus {
    pub resolution: SpuStatusResolution,
}

impl fmt::Display for SpuStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl Default for SpuStatus {
    fn default() -> Self {
        SpuStatus {
            resolution: SpuStatusResolution::default(),
        }
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

    /// Checks if resoultion is marked online. true for online, false otherwise
    pub fn is_online(&self) -> bool {
        self.resolution == SpuStatusResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution == SpuStatusResolution::Offline
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

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SpuStatusResolution {
    Online,
    Offline,
    Init,
}

impl Default for SpuStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}
