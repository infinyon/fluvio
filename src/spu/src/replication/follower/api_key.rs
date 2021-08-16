use dataplane::core::{Encoder, Decoder};

#[repr(u16)]
#[derive(PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum FollowerPeerApiEnum {
    SyncRecords = 0,
    InvalidOffsetRequest = 1
}

impl Default for FollowerPeerApiEnum {
    fn default() -> Self {
        Self::SyncRecords
    }
}
