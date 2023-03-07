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
    #[fluvio(tag = 18)]
    ApiVersion = 18, // VERSIONS_API_KEY
    #[fluvio(tag = 1001)]
    Create = 1001,
    #[fluvio(tag = 1002)]
    Delete = 1002,
    #[fluvio(tag = 1003)]
    List = 1003,
    #[fluvio(tag = 1004)]
    Watch = 1004,
}

impl Default for AdminPublicApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
