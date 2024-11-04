use fluvio_stream_model::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::MirrorSpec;
use super::MirrorStatus;

const REMOTE_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Mirror",
        plural: "mirrors",
        singular: "mirror",
    },
};

impl Spec for MirrorSpec {
    type Header = DefaultHeader;
    type Status = MirrorStatus;
    fn metadata() -> &'static Crd {
        &REMOTE_API
    }
}

impl Status for MirrorStatus {}

#[cfg(test)]
mod test_v1_spec {
    use std::{io::BufReader, fs::File};
    use fluvio_stream_model::k8_types::K8Obj;
    use crate::mirror::Remote;

    use super::MirrorSpec;
    use super::super::MirrorType;

    type K8RemoteSpec = K8Obj<MirrorSpec>;

    #[test]
    fn read_k8_mirror_json_v1() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_mirror_v1.json").expect("spec"));
        let cluster: K8RemoteSpec = serde_json::from_reader(reader).expect("failed to parse topic");
        assert_eq!(cluster.metadata.name, "offshore");
        assert_eq!(
            cluster.spec.mirror_type,
            MirrorType::Remote(Remote {
                id: "offshore-edge-1".to_owned(),
            })
        );
    }
}
