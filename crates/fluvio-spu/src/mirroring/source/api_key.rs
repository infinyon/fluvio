use fluvio_protocol::{Encoder, Decoder};

#[repr(u16)]
#[derive(Eq, PartialEq, Debug, Encoder, Decoder, Clone, Copy, Default)]
#[fluvio(encode_discriminant)]
pub enum MirrorSourceApiEnum {
    #[default]
    SyncRecords = 0,
}
