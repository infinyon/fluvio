use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

/// Api Key for Spu Server API
#[fluvio_kf(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum SpuServerApiKey {
    // Mixed
    ApiVersion = 18,

    // Kafka
    KfProduce = 0,
    KfFetch = 1,

    FlvFetchOffsets = 1002,
    StreamFetch = 1003,
    RegisterSyncReplicaRequest = 1004,
}

impl Default for SpuServerApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
