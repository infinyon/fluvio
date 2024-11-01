use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::PartitionStatus;
use super::PartitionSpec;

const PARTITION_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Partition",
        plural: "partitions",
        singular: "partition",
    },
};

impl Spec for PartitionSpec {
    type Header = DefaultHeader;
    type Status = PartitionStatus;
    fn metadata() -> &'static Crd {
        &PARTITION_API
    }
}

impl Status for PartitionStatus {}

#[cfg(test)]
mod test_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;

    use crate::partition::{HomePartitionConfig, PartitionMirrorConfig};

    use super::PartitionSpec;

    type K8PartitionSpec = K8Obj<PartitionSpec>;

    #[test]
    fn read_k8_partition_mirror_json() {
        let reader: BufReader<File> =
            BufReader::new(File::open("tests/k8_partition_mirror_v1.json").expect("spec"));
        let partition: K8PartitionSpec =
            serde_json::from_reader(reader).expect("failed to parse partiton");
        assert_eq!(partition.metadata.name, "test-0");
        assert_eq!(partition.spec.leader, 5001);
        assert!(partition.spec.replicas.is_empty());
        let mirror = partition.spec.mirror.unwrap();
        assert_eq!(
            mirror,
            PartitionMirrorConfig::Home(HomePartitionConfig {
                remote_cluster: "boat1".to_string(),
                remote_replica: "boats-0".to_string(),
                ..Default::default()
            })
        );
    }
}
