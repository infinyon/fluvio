use fluvio_protocol::{Encoder, Decoder};
use serde::{Serialize, Deserialize};

#[derive(Debug, Encoder, Decoder, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[fluvio(encode_discriminant)]
#[repr(u8)]
pub enum Isolation {
    ReadUncommitted = 0,
    ReadCommitted = 1,
}

impl Default for Isolation {
    fn default() -> Self {
        Isolation::ReadUncommitted
    }
}
