//!
//! # Topic Status
//!
//! Interface to the Topic metadata status in K8 key value store
//!
use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use k8_obj_metadata::Status;

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatus {
    pub resolution: TopicStatusResolution,
    pub replica_map: BTreeMap<i32, Vec<i32>>,
    pub reason: String,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum TopicStatusResolution {
    Init,                  // initial state
    Pending,               // waiting for resources (spus)
    InsufficientResources, // out of sync with partition/replication_factor
    InvalidConfig,         // invalid configuration
    Provisioned,           // spu allocated
}

impl Default for TopicStatusResolution {
    fn default() -> Self {
        TopicStatusResolution::Init
    }
}

impl Status for TopicStatus {}

#[cfg(feature = "flv")]
mod convert {

    use flv_metadata::topic::TopicStatus as FlvTopicStatus;
    use flv_metadata::topic::TopicResolution as FlvTopicStatusResolution;
    use super::*;

    impl Into<FlvTopicStatus> for TopicStatus {
        fn into(self) -> FlvTopicStatus {
        
            FlvTopicStatus {
                resolution: self.resolution.into(),
                replica_map: self.replica_map.clone(),
                reason: self.reason.clone(),
            }
        }
    }

    impl Into<FlvTopicStatusResolution> for TopicStatusResolution {
        fn into(self) -> FlvTopicStatusResolution {
            match self {
                Self::Provisioned => FlvTopicStatusResolution::Provisioned,
                Self::Init => FlvTopicStatusResolution::Init,
                Self::Pending => FlvTopicStatusResolution::Pending,
                Self::InsufficientResources => FlvTopicStatusResolution::InsufficientResources,
                Self::InvalidConfig => FlvTopicStatusResolution::InvalidConfig
            }
        }
    }



    impl From<FlvTopicStatus> for TopicStatus {
        fn from(status: FlvTopicStatus) -> Self {
        
            Self {
                resolution: status.resolution.into(),
                replica_map: status.replica_map.clone(),
                reason: status.reason.clone(),
            }
        }
    }

    impl From<FlvTopicStatusResolution> for TopicStatusResolution {
        fn from(status: FlvTopicStatusResolution) -> Self {
            match status {
                FlvTopicStatusResolution::Provisioned => Self::Provisioned,
                FlvTopicStatusResolution::Init => Self::Init,
                FlvTopicStatusResolution::Pending => Self::Pending,
                FlvTopicStatusResolution::InsufficientResources => Self::InsufficientResources,
                FlvTopicStatusResolution::InvalidConfig => Self::InvalidConfig,
            }
        }
    }
}
