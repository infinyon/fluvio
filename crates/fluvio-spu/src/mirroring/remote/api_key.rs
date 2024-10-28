use fluvio_protocol::{Encoder, Decoder};

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy, Default)]
#[fluvio(encode_discriminant)]
pub enum MirrorRemoteApiEnum {
    #[default]
    SyncRecords = 0,
    UpdateEdgeOffset = 1,
}
