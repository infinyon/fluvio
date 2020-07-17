//!
//! # SC Api Keys
//!
//! Stores Api Keys supported by the SC.
//!

use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

/// API call from client to SPU
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[repr(u16)]
pub enum AdminPublicApiKey {
    // Mixed
    ApiVersion = 18,


    Create = 1001,
    Delete = 1002,
    List = 1003,
    WatchMetadata = 2000
}

impl Default for AdminPublicApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
