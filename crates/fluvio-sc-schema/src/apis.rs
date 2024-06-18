//!
//! # SC Api Keys
//!
//! Stores Api Keys supported by the SC.
//!

use fluvio_protocol::{Encoder, Decoder};

// Make sure that the ApiVersion variant matches dataplane's API_VERSIONS_KEY
static_assertions::const_assert_eq!(
    fluvio_protocol::link::versions::VERSIONS_API_KEY,
    AdminPublicApiKey::ApiVersion as u16,
);

/// API call from client to SPU
#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum AdminPublicApiKey {
    ApiVersion = 18, // VERSIONS_API_KEY
    Create = 1001,
    Delete = 1002,
    List = 1003,
    Watch = 1004,
    Mirroring = 1005,
    Update = 1006,
}

impl Default for AdminPublicApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
