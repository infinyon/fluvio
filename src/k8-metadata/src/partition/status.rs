//!
//! # Partition Status
//!
//! Interface to the Partition metadata status in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use k8_obj_metadata::Status;

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStatus {
    pub resolution: PartitionResolution,
    pub leader: ReplicaStatus,
    pub replicas: Vec<ReplicaStatus>,
    pub lsr: u32,
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    pub spu: i32,
    pub hw: i64,
    pub leo: i64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum PartitionResolution {
    Offline, // no leader
    Online,  // leader is available
    LeaderOffline,
    ElectionLeaderFound,
}

impl Default for PartitionResolution {
    fn default() -> Self {
        PartitionResolution::Offline
    }
}

impl Status for PartitionStatus {}

#[cfg(feature = "flv")]
mod convert {

    use std::convert::Into;

    use flv_metadata::partition::PartitionStatus as FlvPartitionStatus;
    use flv_metadata::partition::ReplicaStatus as FlvReplicaStatus;
    use flv_metadata::partition::PartitionResolution as FlvPartitionResolution;

    use super::*;

    impl Into<FlvPartitionStatus> for PartitionStatus {
        fn into(self) -> FlvPartitionStatus {
            FlvPartitionStatus {
                resolution: self.resolution.into(),
                leader: self.leader.into(),
                replicas: self.replicas.into_iter().map(|lrs| lrs.into()).collect(),
                lsr: self.lsr,
            }
        }
    }

    impl From<FlvPartitionStatus> for PartitionStatus {
        fn from(status: FlvPartitionStatus) -> Self {
            Self {
                resolution: status.resolution.into(),
                leader: status.leader.into(),
                replicas: status.replicas.into_iter().map(|lrs| lrs.into()).collect(),
                lsr: status.lsr.into(),
            }
        }
    }

    impl Into<FlvPartitionResolution> for PartitionResolution {
        fn into(self) -> FlvPartitionResolution {
            match self {
                Self::Offline => FlvPartitionResolution::Offline,
                Self::Online => FlvPartitionResolution::Online,
                Self::ElectionLeaderFound => FlvPartitionResolution::ElectionLeaderFound,
                Self::LeaderOffline => FlvPartitionResolution::LeaderOffline,
            }
        }
    }

    impl From<FlvPartitionResolution> for PartitionResolution {
        fn from(resolution: FlvPartitionResolution) -> Self {
            match resolution {
                FlvPartitionResolution::Offline => Self::Offline,
                FlvPartitionResolution::Online => Self::Online,
                FlvPartitionResolution::LeaderOffline => Self::LeaderOffline,
                FlvPartitionResolution::ElectionLeaderFound => Self::ElectionLeaderFound,
            }
        }
    }

    impl Into<FlvReplicaStatus> for ReplicaStatus {
        fn into(self) -> FlvReplicaStatus {
            FlvReplicaStatus {
                spu: self.spu,
                hw: self.hw,
                leo: self.leo,
            }
        }
    }

    impl From<FlvReplicaStatus> for ReplicaStatus {
        fn from(status: FlvReplicaStatus) -> Self {
            Self {
                spu: status.spu,
                hw: status.hw,
                leo: status.leo,
            }
        }
    }
}
