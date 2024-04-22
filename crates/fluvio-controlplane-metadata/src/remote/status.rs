use std::time::Duration;
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

impl RemoteStatus {
    #[cfg(feature = "use_serde")]
    pub fn last_seen(&self, since: Duration) -> String {
        use humantime_serde::re::humantime;

        let since_sec = since.as_secs();

        if self.connection_stat.last_seen == 0 {
            return "-".to_string();
        }

        let last_seen_sec =
            std::time::Duration::from_millis(self.connection_stat.last_seen).as_secs();
        humantime::Duration::from(std::time::Duration::from_secs(since_sec - last_seen_sec))
            .to_string()
    }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_last_seen() {
        let status = RemoteStatus {
            pairing: RemotePairStatus::Succesful,
            connection_status: ConnectionStatus::Online,
            connection_stat: ConnectionStat {
                last_seen: 1713902927812,
            },
        };

        let since = Duration::from_millis(1713902932152);
        let last_seen = status.last_seen(since);
        assert_eq!(last_seen, "5s");

        let default_status = RemoteStatus {
            pairing: RemotePairStatus::Succesful,
            connection_status: ConnectionStatus::Online,
            connection_stat: ConnectionStat { last_seen: 0 },
        };
        let last_seen = default_status.last_seen(since);
        assert_eq!(last_seen, "-");
    }
}
