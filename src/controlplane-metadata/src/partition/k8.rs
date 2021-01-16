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
