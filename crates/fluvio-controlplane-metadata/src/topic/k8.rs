use crate::k8_types::{Crd, GROUP, CrdNames, Spec, Status, DefaultHeader};

use super::TopicStatus;
use super::TopicSpec;

const TOPIC_V2_API: Crd = Crd {
    group: GROUP,
    version: "v2",
    names: CrdNames {
        kind: "Topic",
        plural: "topics",
        singular: "topic",
    },
};

impl Status for TopicStatus {}

impl Spec for TopicSpec {
    type Status = TopicStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &TOPIC_V2_API
    }
}
