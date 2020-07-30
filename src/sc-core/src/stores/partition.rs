pub use flv_metadata_cluster::partition::store::*;
pub use flv_metadata_cluster::partition::*;
pub use flv_metadata_cluster::store::k8::K8MetaItem;

pub type PartitionAdminMd = PartitionMetadata<K8MetaItem>;
pub type PartitionAdminStore = PartitionLocalStore<K8MetaItem>;
