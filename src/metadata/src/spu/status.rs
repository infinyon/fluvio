//!
//! # Spu Status
//!
//! Spu Status metadata information cached locally.
//!
use std::fmt;


use kf_protocol::derive::{Decode, Encode};

use k8_metadata::spu::SpuStatus as K8SpuStatus;
use k8_metadata::spu::SpuStatusResolution as K8SpuStatusResolution;


// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct SpuStatus {
    pub resolution: SpuResolution,
}

impl fmt::Display for SpuStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"{:#?}",self.resolution)
    }
}


// -----------------------------------
// Implementation - SpuStatus
// -----------------------------------

impl From<K8SpuStatus> for SpuStatus {
    fn from(kv_status: K8SpuStatus) -> Self {
        SpuStatus { 
            resolution: match kv_status.resolution {
                K8SpuStatusResolution::Online => SpuResolution::Online,
                K8SpuStatusResolution::Offline => SpuResolution::Offline,
                K8SpuStatusResolution::Init => SpuResolution::Init
            },
        }
    }
}

impl From<SpuStatus> for K8SpuStatus {
    fn from(status: SpuStatus) -> K8SpuStatus {
        K8SpuStatus {
            resolution: (match status.resolution {
                SpuResolution::Online => K8SpuStatusResolution::Online,
                SpuResolution::Offline => K8SpuStatusResolution::Offline,
                SpuResolution::Init => K8SpuStatusResolution::Init
            }),
        }
    }
}

impl Default for SpuStatus {
    fn default() -> Self {
        SpuStatus {
            resolution: SpuResolution::default(),
        }
    }
}

impl SpuStatus {
    /// Resolution to string label
    pub fn resolution_label(&self) -> &'static str {
        match self.resolution {
            SpuResolution::Online => "online",
            SpuResolution::Offline => "offline",
            SpuResolution::Init => "Init"
        }
    }

    /// Checks if resoultion is marked online. true for online, false otherwise
    pub fn is_online(&self) -> bool {
        self.resolution == SpuResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution == SpuResolution::Offline
    }

    /// Set resolution to status to online
    pub fn set_online(&mut self) {
        self.resolution = SpuResolution::Online;
    }

    /// Set resolution to status to offline
    pub fn set_offline(&mut self) {
        self.resolution = SpuResolution::Offline;
    }
}



#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum SpuResolution {
    Online,
    Offline,
    Init
}



// -----------------------------------
// Implementation - SpuResolution
// -----------------------------------

impl Default for SpuResolution {
    fn default() -> Self {
        SpuResolution::Init
    }
}

