use dataplane::derive::Encoder;
use dataplane::derive::Decoder;

/// Api Key for Spu Client API (from server to client)
#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SpuClientApiKey {
    ReplicaOffsetUpdate = 1001,
}

impl Default for SpuClientApiKey {
    fn default() -> Self {
        Self::ReplicaOffsetUpdate
    }
}
