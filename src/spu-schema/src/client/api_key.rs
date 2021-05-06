use dataplane::derive::Encode;
use dataplane::derive::Decode;

/// Api Key for Spu Client API (from server to client)
#[repr(u16)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SpuClientApiKey {
    ReplicaOffsetUpdate = 1001,
}

impl Default for SpuClientApiKey {
    fn default() -> Self {
        Self::ReplicaOffsetUpdate
    }
}
