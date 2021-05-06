use dataplane::derive::{Encode, Decode};

#[repr(u16)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum LeaderPeerApiEnum {
    UpdateOffsets = 0,
}

impl Default for LeaderPeerApiEnum {
    fn default() -> Self {
        Self::UpdateOffsets
    }
}
