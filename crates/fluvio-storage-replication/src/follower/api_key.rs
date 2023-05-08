use fluvio_protocol::{Encoder, Decoder};

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
pub enum FollowerPeerApiEnum {
    SyncRecords = 0,
    RejectedOffsetRequest = 1,
}

impl Default for FollowerPeerApiEnum {
    fn default() -> Self {
        Self::SyncRecords
    }
}
