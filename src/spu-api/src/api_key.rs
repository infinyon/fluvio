
use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

#[derive(PartialEq, Debug, Encode, Decode,  Clone, Copy)]
#[repr(u16)]
pub enum SpuApiKey {
    // Mixed
    ApiVersion = 18,

    // Kafka
    KfProduce = 0,
    KfFetch = 1,

    // Fluvio
    FlvFetchLocalSpu = 1001,
    FlvFetchOffsets = 1002,
    FlvContinuousFetch = 1003
}

impl Default for SpuApiKey {
    fn default() -> SpuApiKey {
        SpuApiKey::ApiVersion
    }
}
