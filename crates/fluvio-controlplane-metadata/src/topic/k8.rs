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
mod test_col_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;


    use crate::topic::schema::ColumnType;

    use super::TopicSpec;

    type K8TopicSpec = K8Obj<TopicSpec>;

    #[test]
    fn topic_with_col_from_k8() {
        let reader = BufReader::new(File::open("tests/topic_col.yaml").expect("v2 not found"));
        let topic_k8: K8TopicSpec = serde_yaml::from_reader(reader).expect("failed to parse sm k8");

        let metadata = topic_k8.metadata;
        assert_eq!(metadata.name, "vehicle");

        let col_route = &topic_k8.spec.get_schema().get_columns()[0];
        assert_eq!(col_route.name, "route");
        assert_eq!(col_route.ty, ColumnType::String);
    }
}
