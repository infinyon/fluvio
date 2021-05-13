//!
//! # SC Api Keys
//!
//! Stores Api Keys supported by the SC.
//!

use dataplane::derive::{Decode, Encode};

// Make sure that the ApiVersion variant matches dataplane's API_VERSIONS_KEY
static_assertions::const_assert_eq!(
    dataplane::versions::VERSIONS_API_KEY,
    AdminPublicApiKey::ApiVersion as u16,
);

/// API call from client to SPU
#[repr(u16)]
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum AdminPublicApiKey {
    ApiVersion = 18, // VERSIONS_API_KEY
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
