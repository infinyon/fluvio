//!
//! # AuthTokens
//!
//! Fields used by multiple Auth Token APIs
//!

use kf_protocol::derive::{Encode, Decode};

// -----------------------------------
// FlvTokenType
// -----------------------------------

/// Fluvio SPU type: Custom, Managed, or Any
#[derive(Decode, Encode, Debug, Clone, PartialEq)]
pub enum FlvTokenType {
    Any,
    Custom,
    Managed,
}

// -----------------------------------
// Defaults
// -----------------------------------

impl ::std::default::Default for FlvTokenType {
    fn default() -> Self {
        FlvTokenType::Any
    }
}
