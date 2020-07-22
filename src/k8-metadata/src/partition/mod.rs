mod spec;
mod status;

pub use self::spec::PartitionSpec;
pub use self::status::PartitionStatus;
pub use self::status::ReplicaStatus;
pub use self::status::PartitionResolution;

use k8_obj_metadata::Crd;
use k8_obj_metadata::CrdNames;
use k8_obj_metadata::GROUP;
use k8_obj_metadata::V1;

const PARTITION_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Partition",
        plural: "partitions",
        singular: "partition",
    },
};
