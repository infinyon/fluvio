use fluvio_protocol::derive::Encode;
use fluvio_protocol::derive::Decode;

/// Api Key for Spu Client API (from server to client)
#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum SpuClientApiKey {
    ReplicaOffsetUpdate = 1001,
}

impl Default for SpuClientApiKey {
    fn default() -> Self {
        Self::ReplicaOffsetUpdate
    }
}
