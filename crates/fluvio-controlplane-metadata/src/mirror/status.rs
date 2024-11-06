use fluvio_protocol::{Encoder, Decoder};

#[derive(Encoder, Decoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct MirrorStatus {
    pub connection_status: ConnectionStatus,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub pairing_sc: MirrorPairStatus,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub pairing_spu: MirrorPairStatus,
    pub connection_stat: ConnectionStat,
}

impl MirrorStatus {
    pub fn new(
        pairing: MirrorPairStatus,
        connection_status: ConnectionStatus,
        last_seen: u64,
    ) -> Self {
        Self {
            pairing_sc: pairing,
            pairing_spu: MirrorPairStatus::Waiting,
            connection_status,
            connection_stat: ConnectionStat { last_seen },
        }
    }

    pub fn new_by_spu(pairing_spu: MirrorPairStatus, last_seen: u64) -> Self {
        Self {
            pairing_spu,
            connection_stat: ConnectionStat { last_seen },
            ..Default::default()
        }
    }

    pub fn merge_from_sc(&mut self, other: Self) {
        self.pairing_sc = other.pairing_sc;
        self.connection_status = other.connection_status;
        self.connection_stat = other.connection_stat;
    }

    pub fn merge_from_spu(&mut self, other: Self) {
        self.pairing_spu = other.pairing_spu;
        self.connection_stat = other.connection_stat;
    }

    pub fn pair_errors(self) -> String {
        match (self.pairing_sc, self.pairing_spu) {
            (MirrorPairStatus::DetailFailure(sc_err), MirrorPairStatus::DetailFailure(spu_err)) => {
                format!("SC: {} - SPU: {}", sc_err, spu_err)
            }
            (MirrorPairStatus::DetailFailure(sc_err), _) => sc_err,
            (_, MirrorPairStatus::DetailFailure(spu_err)) => spu_err,
            _ => "-".to_string(),
        }
    }
}

#[derive(Encoder, Decoder, Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MirrorPairStatus {
    #[default]
    #[fluvio(tag = 0)]
    Waiting,
    #[fluvio(tag = 1)]
    Successful,
    #[fluvio(tag = 2)]
    Failed,
    #[fluvio(tag = 3)]
    Disabled,
    #[fluvio(tag = 4)]
    Unauthorized,
    #[fluvio(tag = 5, min_version = 17)]
    DetailFailure(String),
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

impl MirrorStatus {
    #[cfg(feature = "use_serde")]
    pub fn last_seen(&self, since: std::time::Duration) -> String {
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

impl std::fmt::Display for MirrorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:SPU:{}:SC:{}",
            self.connection_status, self.pairing_spu, self.pairing_sc
        )
    }
}

impl std::fmt::Display for MirrorPairStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            MirrorPairStatus::Successful => "Connected",
            MirrorPairStatus::Disabled => "Disabled",
            MirrorPairStatus::Failed => "Failed",
            MirrorPairStatus::Waiting => "Waiting",
            MirrorPairStatus::Unauthorized => "Unauthorized",
            MirrorPairStatus::DetailFailure(_) => "Failed", // the msg is showed with pair_errors
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
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_last_seen() {
        let status = MirrorStatus {
            pairing_sc: MirrorPairStatus::Successful,
            pairing_spu: MirrorPairStatus::Waiting,
            connection_status: ConnectionStatus::Online,
            connection_stat: ConnectionStat {
                last_seen: 1713902927812,
            },
        };

        let since = Duration::from_millis(1713902932152);
        let last_seen = status.last_seen(since);
        assert_eq!(last_seen, "5s");

        let default_status = MirrorStatus {
            pairing_sc: MirrorPairStatus::Successful,
            pairing_spu: MirrorPairStatus::Waiting,
            connection_status: ConnectionStatus::Online,
            connection_stat: ConnectionStat { last_seen: 0 },
        };
        let last_seen = default_status.last_seen(since);
        assert_eq!(last_seen, "-");
    }
}
