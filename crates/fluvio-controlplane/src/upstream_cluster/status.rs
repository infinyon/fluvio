use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct UpstreamClusterStatus {
    pairing: UpStreamPairStatus,
    connection_status: ConnectionStatus,
    connection_stat: ConnectionStat,
}

impl fmt::Display for UpstreamClusterStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UpstreamStatus: {:#?}", self)
    }
}

#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UpStreamPairStatus {
    #[default]
    #[fluvio(tag = 0)]
    Request,
    #[fluvio(tag = 1)]
    Succesful,
    #[fluvio(tag = 2)]
    Failed,
    #[fluvio(tag = 3)]
    Disabled,
}

#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConnectionStatus {
    #[default]
    #[fluvio(tag = 0)]
    Offline,
    #[fluvio(tag = 1)]
    Online,
}

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ConnectionStat {
    pub last_sent: u64, // number of milliseconds since last sent
}
