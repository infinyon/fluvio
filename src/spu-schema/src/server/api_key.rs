use dataplane::derive::{Decode, Encode};

// Make sure that the ApiVersion variant matches dataplane's API_VERSIONS_KEY
static_assertions::const_assert_eq!(
    dataplane::versions::VERSIONS_API_KEY,
    SpuServerApiKey::ApiVersion as u16,
);

/// Api Key for Spu Server API
#[repr(u16)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SpuServerApiKey {
    ApiVersion = 18, // API_VERSIONS_KEY

    Produce = 0,
    Fetch = 1,

    FetchOffsets = 1002,
    StreamFetch = 1003,
    UpdateOffsets = 1005,
}

impl Default for SpuServerApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
