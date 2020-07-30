pub use flv_metadata_cluster::topic::store::*;
pub use flv_metadata_cluster::topic::*;
pub use flv_metadata_cluster::store::k8::K8MetaItem;

pub type TopicAdminStore = TopicLocalStore<K8MetaItem>;
pub type TopicAdminMd = TopicMetadata<K8MetaItem>;
