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
    ApiVersion = 18, // API_VERSIONS_KEY

    Produce = 0,
    #[cfg(feature = "file")]
    Fetch = 1,

    FetchOffsets = 1002,
    StreamFetch = 1003,
    UpdateOffsets = 1005,
    UpdateConsumerOffset = 1006,
    DeleteConsumerOffset = 1007,
    FetchConsumerOffsets = 1008,

    StartMirror = 2000,
}

impl Default for SpuServerApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
