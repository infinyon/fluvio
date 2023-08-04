pub mod store;

pub use store::*;
pub use fluvio_controlplane_metadata::topic::*;
pub use fluvio_controlplane_metadata::store::k8::K8MetaItem;

pub type TopicAdminStore = TopicLocalStore<K8MetaItem>;
pub type TopicAdminMd = TopicMetadata<K8MetaItem>;

