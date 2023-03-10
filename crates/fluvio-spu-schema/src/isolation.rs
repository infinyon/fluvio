use fluvio_protocol::{Encoder, Decoder};
use serde::{Serialize, Deserialize};

#[derive(Debug, Encoder, Decoder, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[fluvio(encode_discriminant)]
#[repr(u8)]
#[derive(Default)]
pub enum Isolation {
    #[default]
    ReadUncommitted = 0,
    ReadCommitted = 1,
}
