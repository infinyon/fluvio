use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]

pub struct RemoteClusterSpec {
    pub remote_type: RemoteClusterType,
    pub key_pair: KeyPair,
}

impl fmt::Display for RemoteClusterSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RemoteCluster: {:?}", self)
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteClusterType {
    #[default]
    #[fluvio(tag = 0)]
    MirrorEdge,
}

impl fmt::Display for RemoteClusterType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = match self {
            RemoteClusterType::MirrorEdge => "mirror-edge",
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
