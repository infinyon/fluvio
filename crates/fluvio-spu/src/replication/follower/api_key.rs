use fluvio_protocol::{Encoder, Decoder};

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy)]
#[fluvio(encode_discriminant)]
#[derive(Default)]
pub enum FollowerPeerApiEnum {
    #[default]
    SyncRecords = 0,
    RejectedOffsetRequest = 1,
}
