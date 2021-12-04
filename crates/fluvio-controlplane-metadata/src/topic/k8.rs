use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::PartitionMaps;
use super::ReplicaSpec;
use super::TopicReplicaParam;
use super::TopicStatus;
use super::TopicSpec;

const TOPIC_V1_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Topic",
        plural: "topics",
        singular: "topic",
    },
};

const TOPIC_V2_API: Crd = Crd {
    group: GROUP,
    version: "v2",
    names: CrdNames {
        kind: "Topic",
        plural: "topics",
        singular: "topic",
    },
};

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "type")
)]
pub enum TopicSpecV1 {
    Assigned(PartitionMaps),
    Computed(TopicReplicaParam),
}

// -----------------------------------
// Implementation
// -----------------------------------
impl Default for TopicSpecV1 {
    fn default() -> Self {
        Self::Assigned(PartitionMaps::default())
    }
}

impl Spec for TopicSpecV1 {
    type Status = TopicStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &TOPIC_V1_API
    }
}

impl Status for TopicStatus {}

impl Spec for TopicSpec {
    type Status = TopicStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &TOPIC_V2_API
    }
}

impl From<TopicSpecV1> for TopicSpec {
    fn from(v1: TopicSpecV1) -> Self {
        TopicSpec {
            replicas: v1.into(),
            ..Default::default()
        }
    }
}

impl From<TopicSpecV1> for ReplicaSpec {
    fn from(v1: TopicSpecV1) -> Self {
        match v1 {
            TopicSpecV1::Assigned(partition_maps) =>  ReplicaSpec::Assigned(partition_maps),
            TopicSpecV1::Computed(topic_replica_param) => ReplicaSpec::Computed(topic_replica_param),
        }
    }
}
