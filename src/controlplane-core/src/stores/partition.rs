pub use fluvio_metadata::partition::store::*;
pub use fluvio_metadata::partition::*;
pub use fluvio_metadata::store::k8::K8MetaItem;

pub type PartitionAdminMd = PartitionMetadata<K8MetaItem>;
pub type PartitionAdminStore = PartitionLocalStore<K8MetaItem>;
