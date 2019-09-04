//!
//! # Auth Token Status
//!
//! Auth Token Status metadata information cached locally.
//!
use kf_protocol::derive::{Decode, Encode};

use k8_metadata::auth_token::AuthTokenStatus as K8AuthTokenStatus;
use k8_metadata::auth_token::TokenResolution as K8TokenResolution;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub struct AuthTokenStatus {
    pub resolution: TokenResolution,
    pub reason: String,
}

#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum TokenResolution {
    Ok,      // operational
    Init,    // initializing
    Invalid, // inactive
}

// -----------------------------------
// Encode - from K8 AuthTokenStatus
// -----------------------------------

impl From<K8AuthTokenStatus> for AuthTokenStatus {
    fn from(k8_status: K8AuthTokenStatus) -> Self {
        AuthTokenStatus {
            resolution: k8_status.resolution.into(),
            reason: k8_status.reason.clone(),
        }
    }
}

impl From<K8TokenResolution> for TokenResolution {
    fn from(k8_token_resolution: K8TokenResolution) -> Self {
        match k8_token_resolution {
            K8TokenResolution::Ok => TokenResolution::Ok,
            K8TokenResolution::Init => TokenResolution::Init,
            K8TokenResolution::Invalid => TokenResolution::Invalid,
        }
    }
}

impl Into<K8AuthTokenStatus> for AuthTokenStatus {
    fn into(self) -> K8AuthTokenStatus {
        K8AuthTokenStatus {
            resolution: self.resolution.into(),
            reason: self.reason.clone(),
        }
    }
}

impl Into<K8TokenResolution> for TokenResolution {
    fn into(self) -> K8TokenResolution {
        match self {
            TokenResolution::Ok => K8TokenResolution::Ok,
            TokenResolution::Init => K8TokenResolution::Init,
            TokenResolution::Invalid => K8TokenResolution::Invalid,
        }
    }
}

// -----------------------------------
// Defaults
// -----------------------------------

impl ::std::default::Default for AuthTokenStatus {
    fn default() -> Self {
        AuthTokenStatus {
            resolution: TokenResolution::default(),
            reason: "".to_owned(),
        }
    }
}


impl ::std::default::Default for TokenResolution {
    fn default() -> Self {
        TokenResolution::Init
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

impl AuthTokenStatus {
    pub fn resolution_label(resolution: &TokenResolution) -> &'static str {
        match resolution {
            TokenResolution::Ok => "ok",
            TokenResolution::Init => "initializing",
            TokenResolution::Invalid => "invalid",
        }
    }

    // -----------------------------------
    // Resolution
    // -----------------------------------
    
    pub fn is_resolution_ok(&self) -> bool {
        self.resolution == TokenResolution::Ok
    }

    pub fn is_resolution_init(&self) -> bool {
        self.resolution == TokenResolution::Init
    }

    pub fn next_resolution_ok(&mut self) {
        self.resolution = TokenResolution::Ok;
        self.reason = "".to_owned();
    }

    pub fn next_resolution_invalid(&mut self, reason: String) {
        self.resolution = TokenResolution::Invalid;
        self.reason = reason;
    }
}
