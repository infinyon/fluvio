use fluvio_protocol::{Encoder, Decoder};

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum LeaderPeerApiEnum {
    UpdateOffsets = 0,
}

impl Default for LeaderPeerApiEnum {
    fn default() -> Self {
        Self::UpdateOffsets
    }
}
