use dataplane::derive::{Encode, Decode};

#[repr(u16)]
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum FollowerPeerApiEnum {
    SyncRecords = 0,
}

impl Default for FollowerPeerApiEnum {
    fn default() -> Self {
        Self::SyncRecords
    }
}
