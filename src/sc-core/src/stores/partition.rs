pub use flv_metadata_cluster::partition::store::*;
pub use flv_metadata_cluster::partition::*;

use super::*;

pub type PartitionAdminMd = PartitionMetadata<K8MetaItem>;
pub type PartitionAdminStore = PartitionLocalStore<K8MetaItem>;

impl K8ExtendedSpec for PartitionSpec {
    type K8Spec = Self;
    type K8Status = Self::Status;
}
