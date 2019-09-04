use serde::{Serialize, Deserialize};

use kf_protocol_derive::Decode;
use kf_protocol_derive::Encode;

#[derive(Debug, Encode, Serialize, Deserialize, Decode, Clone)]
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
