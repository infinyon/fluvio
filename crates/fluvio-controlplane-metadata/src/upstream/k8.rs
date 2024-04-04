use fluvio_stream_model::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::{UpstreamSpec, UpstreamStatus};

const UPSTREAM_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Upstream",
        plural: "upstreams",
        singular: "upstream",
    },
};

impl Spec for UpstreamSpec {
    type Header = DefaultHeader;
    type Status = UpstreamStatus;
    fn metadata() -> &'static Crd {
        &UPSTREAM_API
    }
}

impl Status for UpstreamStatus {}

#[cfg(test)]
mod test_v1_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;

    use super::UpstreamSpec;

    type K8UpStreamSpec = K8Obj<UpstreamSpec>;

    #[test]
    fn read_k8_upstream_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_upstream_v1.json").expect("spec"));
        let cluster: K8UpStreamSpec =
            serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "somecloud");
        assert_eq!(cluster.spec.target.endpoint, "somecloud.com:9003");
        assert_eq!(cluster.spec.source_id, "edge1")
    }

    #[test]
    fn read_k8_upstream_yaml() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_upstream_v1.yaml").expect("spec"));
        let cluster: K8UpStreamSpec =
            serde_yaml::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "somecloud");
        assert_eq!(cluster.spec.target.endpoint, "somecloud.com:9003");
        assert_eq!(cluster.spec.source_id, "edge1")
    }
}
