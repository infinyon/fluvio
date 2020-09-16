use dataplane::derive::Encode;
use dataplane::derive::Decode;

/// Api Key for Spu Server API
#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum SpuServerApiKey {
    // Mixed
    ApiVersion = 18,

    // Kafka
    Produce = 0,
    Fetch = 1,

    FetchOffsets = 1002,
    StreamFetch = 1003,
    RegisterSyncReplicaRequest = 1004,
}

impl Default for SpuServerApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
