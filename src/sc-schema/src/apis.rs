//!
//! # SC Api Keys
//!
//! Stores Api Keys supported by the SC.
//!

use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

/// API call from client to SPU
#[fluvio_kf(encode_discriminant)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[repr(u16)]
pub enum AdminPublicApiKey {
    ApiVersion = 18,

    Create = 1001,
    Delete = 1002,
    List = 1003,
    Watch = 1004,
}

impl Default for AdminPublicApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
