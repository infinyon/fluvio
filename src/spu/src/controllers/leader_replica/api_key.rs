use dataplane::derive::{Encode, Decode};

#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum LeaderPeerApiEnum {
    UpdateOffsets = 0,
}

impl Default for LeaderPeerApiEnum {
    fn default() -> Self {
        Self::UpdateOffsets
    }
}
