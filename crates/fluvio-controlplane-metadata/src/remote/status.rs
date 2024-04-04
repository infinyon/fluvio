use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct RemoteStatus {
    pairing: RemotePairStatus,
    connection_status: ConnectionStatus,
    pub connection_stat: ConnectionStat,
}

#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemotePairStatus {
    #[default]
    #[fluvio(tag = 0)]
    Waiting,
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
    pub last_seen: u64, // number of milliseconds since last seen
}

impl std::fmt::Display for RemoteStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match (self.pairing.clone(), self.connection_status.clone()) {
            (RemotePairStatus::Succesful, ConnectionStatus::Online) => "Online",
            (RemotePairStatus::Failed, ConnectionStatus::Online) => "Failed",
            (RemotePairStatus::Disabled, ConnectionStatus::Online) => "Disabled",
            (RemotePairStatus::Waiting, ConnectionStatus::Online) => "Waiting",
            (RemotePairStatus::Succesful, ConnectionStatus::Offline) => "Offline",
            (RemotePairStatus::Failed, ConnectionStatus::Offline) => "Failed",
            (RemotePairStatus::Disabled, ConnectionStatus::Offline) => "Disabled",
            (RemotePairStatus::Waiting, ConnectionStatus::Offline) => "Waiting",
        };
        write!(f, "{}", status)
    }
}

impl std::fmt::Display for RemotePairStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            RemotePairStatus::Succesful => "successful",
            RemotePairStatus::Disabled => "disabled",
            RemotePairStatus::Failed => "failed",
            RemotePairStatus::Waiting => "waiting",
        };
        write!(f, "{}", status)
    }
}

impl std::fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            ConnectionStatus::Online => "online",
            ConnectionStatus::Offline => "offline",
        };
        write!(f, "{}", status)
    }
}
