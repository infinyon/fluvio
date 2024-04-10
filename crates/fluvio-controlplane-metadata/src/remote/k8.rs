use fluvio_stream_model::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::RemoteSpec;
use super::RemoteStatus;

const REMOTE_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Remote",
        plural: "remotes",
        singular: "remote",
    },
};

impl Spec for RemoteSpec {
    type Header = DefaultHeader;
    type Status = RemoteStatus;
    fn metadata() -> &'static Crd {
        &REMOTE_API
    }
}

impl Status for RemoteStatus {}

#[cfg(test)]
mod test_v1_spec {
    use std::{io::BufReader, fs::File};
    use fluvio_stream_model::k8_types::K8Obj;
    use crate::remote::Edge;

    use super::RemoteSpec;
    use super::super::RemoteType;

    type K8RemoteSpec = K8Obj<RemoteSpec>;

    #[test]
    fn read_k8_remote_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_remote_v1.json").expect("spec"));
        let cluster: K8RemoteSpec = serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "offshore");
        assert_eq!(
            cluster.spec.remote_type,
            RemoteType::Edge(Edge {
                id: "offshore-edge-1".to_owned(),
            })
        );
    }
}
