pub use fluvio_metadata::topic::store::*;
pub use fluvio_metadata::topic::*;
pub use fluvio_metadata::store::k8::K8MetaItem;

pub type TopicAdminStore = TopicLocalStore<K8MetaItem>;
pub type TopicAdminMd = TopicMetadata<K8MetaItem>;
