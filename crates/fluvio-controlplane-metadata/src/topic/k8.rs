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

#[cfg(test)]
mod test_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;

    use crate::topic::ReplicaSpec;

    use super::TopicSpec;

    type K8TopicSpec = K8Obj<TopicSpec>;

    #[test]
    fn read_k8_topic_partition_assignment_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/topic_assignment.json").expect("spec"));
        let topic: K8TopicSpec = serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(topic.metadata.name, "test3");
        assert!(matches!(topic.spec.replicas(), ReplicaSpec::Assigned(_)));
    }
}
