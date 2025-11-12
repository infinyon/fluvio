use fluvio_protocol::{Encoder, Decoder};

/// Api Key for Spu Client API (from server to client)
#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum SpuClientApiKey {
    #[default]
    ReplicaOffsetUpdate = 1001,
}
