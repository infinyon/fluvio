mod spec;
mod status;

pub use self::spec::PartitionSpec;
pub use self::status::PartitionStatus;
pub use self::status::ReplicaStatus;
pub use self::status::PartitionResolution;

use metadata_core::Crd;
use metadata_core::CrdNames;
use metadata_core::GROUP;
use metadata_core::V1;

const PARTITION_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Partition",
        plural: "partitions",
        singular: "partition",
    },
};
