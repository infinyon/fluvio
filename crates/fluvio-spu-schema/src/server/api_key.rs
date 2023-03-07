use fluvio_protocol::{Decoder, Encoder};

// Make sure that the ApiVersion variant matches dataplane's API_VERSIONS_KEY
static_assertions::const_assert_eq!(
    fluvio_protocol::link::versions::VERSIONS_API_KEY,
    SpuServerApiKey::ApiVersion as u16,
);

/// Api Key for Spu Server API
#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SpuServerApiKey {
    #[fluvio(tag = 18)]
    ApiVersion = 18, // API_VERSIONS_KEY
    #[fluvio(tag = 0)]
    Produce = 0,
    #[fluvio(tag = 1)]
    #[cfg(feature = "file")]
    Fetch = 1,
    #[fluvio(tag = 1002)]
    FetchOffsets = 1002,
    #[fluvio(tag = 1003)]
    StreamFetch = 1003,
    #[fluvio(tag = 1005)]
    UpdateOffsets = 1005,
}

impl Default for SpuServerApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
