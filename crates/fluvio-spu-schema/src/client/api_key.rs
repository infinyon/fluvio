use fluvio_protocol::{Encoder, Decoder};

/// Api Key for Spu Client API (from server to client)
#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum SpuClientApiKey {
    #[fluvio(tag = 1001)]
    ReplicaOffsetUpdate = 1001,
}

impl Default for SpuClientApiKey {
    fn default() -> Self {
        Self::ReplicaOffsetUpdate
    }
}
