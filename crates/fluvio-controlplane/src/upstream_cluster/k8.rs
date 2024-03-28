use fluvio_stream_model::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::UpstreamClusterSpec;
use super::UpstreamClusterStatus;

const UPSTREAM_CLUSTER_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "UpstreamCluster",
        plural: "upstreamclusters",
        singular: "upstreamcluster",
    },
};

impl Spec for UpstreamClusterSpec {
    type Header = DefaultHeader;
    type Status = UpstreamClusterStatus;
    fn metadata() -> &'static Crd {
        &UPSTREAM_CLUSTER_API
    }
}

impl Status for UpstreamClusterStatus {}

#[cfg(test)]
mod test_v1_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;

    use super::UpstreamClusterSpec;

    type K8UpStreamClusterSpec = K8Obj<UpstreamClusterSpec>;

    #[test]
    fn read_k8_upstream_cluster_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_upstream_cluster_v1.json").expect("spec"));
        let cluster: K8UpStreamClusterSpec =
            serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "somecloud");
        assert_eq!(cluster.spec.target.endpoint, "somecloud.com:9003");
        assert_eq!(cluster.spec.source_id, "edge1")
    }
}
