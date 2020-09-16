use dataplane::derive::{Encode, Decode};

#[fluvio(encode_discriminant)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum FollowerPeerApiEnum {
    SyncRecords = 0,
}

impl Default for FollowerPeerApiEnum {
    fn default() -> Self {
        Self::SyncRecords
    }
}
