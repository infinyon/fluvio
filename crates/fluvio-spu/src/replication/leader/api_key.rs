use dataplane::core::{Encoder, Decoder};

#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum LeaderPeerApiEnum {
    UpdateOffsets = 0,
    StreamFetch = 1,
}

impl Default for LeaderPeerApiEnum {
    fn default() -> Self {
        Self::UpdateOffsets
    }
}
