use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

/// Api Key for Spu Client API (from server to client)
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
