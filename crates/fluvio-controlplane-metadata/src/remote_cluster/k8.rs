use fluvio_stream_model::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::RemoteClusterSpec;
use super::RemoteClusterStatus;

const REMOTE_CLUSTER_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "RemoteCluster",
        plural: "remoteclusters",
        singular: "remotecluster",
    },
};

impl Spec for RemoteClusterSpec {
    type Header = DefaultHeader;
    type Status = RemoteClusterStatus;
    fn metadata() -> &'static Crd {
        &REMOTE_CLUSTER_API
    }
}

impl Status for RemoteClusterStatus {}

#[cfg(test)]
mod test_v1_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;

    use super::RemoteClusterSpec;
    use super::super::RemoteClusterType;

    type K8RemoteClusterSpec = K8Obj<RemoteClusterSpec>;

    #[test]
    fn read_k8_remote_cluster_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_remote_cluster_v1.json").expect("spec"));
        let cluster: K8RemoteClusterSpec =
            serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "boat1");
        assert_eq!(cluster.spec.remote_type, RemoteClusterType::MirrorEdge);
    }
}
