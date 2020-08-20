pub use fluvio_metadata::spg::*;
pub use fluvio_metadata::spg::store::*;
pub use fluvio_metadata::store::k8::K8MetaItem;

pub type SpgAdminMd = SpuGroupMetadata<K8MetaItem>;
pub type SpgAdminStore = SpuGroupLocalStore<K8MetaItem>;
