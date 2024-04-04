use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]

pub struct RemoteSpec {
    pub remote_type: RemoteType,
    // TODO: we should add auth
    pub key_pair: KeyPair,
}

impl fmt::Display for RemoteSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Remote: {:?}", self)
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteType {
    #[default]
    #[fluvio(tag = 0)]
    Edge,
    #[fluvio(tag = 1)]
    Core,
}

impl fmt::Display for RemoteType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = match self {
            RemoteType::Edge => "edge",
            RemoteType::Core => "core",
        };
        write!(f, "{}", ts)
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct KeyPair {
    pub public_key: String,
    pub private_key: String,
}
