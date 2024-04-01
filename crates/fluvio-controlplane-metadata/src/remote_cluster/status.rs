use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct RemoteClusterStatus {
    pairing: RemoteClusterPairStatus,
    connection_status: ConnectionStatus,
    pub connection_stat: ConnectionStat,
}

#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteClusterPairStatus {
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

impl std::fmt::Display for RemoteClusterStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match (self.pairing.clone(), self.connection_status.clone()) {
            (RemoteClusterPairStatus::Succesful, ConnectionStatus::Online) => "Online",
            (RemoteClusterPairStatus::Failed, ConnectionStatus::Online) => "Failed",
            (RemoteClusterPairStatus::Disabled, ConnectionStatus::Online) => "Disabled",
            (RemoteClusterPairStatus::Waiting, ConnectionStatus::Online) => "Waiting",
            (RemoteClusterPairStatus::Succesful, ConnectionStatus::Offline) => "Offline",
            (RemoteClusterPairStatus::Failed, ConnectionStatus::Offline) => "Failed",
            (RemoteClusterPairStatus::Disabled, ConnectionStatus::Offline) => "Disabled",
            (RemoteClusterPairStatus::Waiting, ConnectionStatus::Offline) => "Waiting",
        };
        write!(f, "{}", status)
    }
}

impl std::fmt::Display for RemoteClusterPairStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            RemoteClusterPairStatus::Succesful => "successful",
            RemoteClusterPairStatus::Disabled => "disabled",
            RemoteClusterPairStatus::Failed => "failed",
            RemoteClusterPairStatus::Waiting => "waiting",
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
