#![allow(clippy::assign_op_pattern)]

//!
//! # Schema Status
//!
//! Schema Status metadata information cached locally.
//!
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SchemaStatus {
    pub resolution: SchemaStatusResolution,
}

impl fmt::Display for SchemaStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl SchemaStatus {
    /// create offline status
    pub fn offline() -> Self {
        Self {
            resolution: SchemaStatusResolution::Offline,
        }
    }
    /// Resolution to string label
    pub fn resolution_label(&self) -> &'static str {
        match self.resolution {
            SchemaStatusResolution::Online => "online",
            SchemaStatusResolution::Offline => "offline",
            SchemaStatusResolution::Init => "Init",
        }
    }

    /// Checks if resolution is marked online. true for online, false otherwise
    pub fn is_online(&self) -> bool {
        self.resolution == SchemaStatusResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution == SchemaStatusResolution::Offline
    }

    pub fn is_init(&self) -> bool {
        self.resolution == SchemaStatusResolution::Init
    }

    /// Set resolution to status to online
    pub fn set_online(&mut self) {
        self.resolution = SchemaStatusResolution::Online;
    }

    /// Set resolution to status to offline
    pub fn set_offline(&mut self) {
        self.resolution = SchemaStatusResolution::Offline;
    }
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SchemaStatusResolution {
    #[fluvio(tag = 0)]
    Online,
    #[fluvio(tag = 1)]
    Offline,
    #[fluvio(tag = 2)]
    Init,
}

impl Default for SchemaStatusResolution {
    fn default() -> Self {
        Self::Init
    }
}
