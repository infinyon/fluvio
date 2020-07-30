pub use flv_metadata_cluster::spg::*;
pub use flv_metadata_cluster::spg::store::*;
pub use flv_metadata_cluster::store::k8::K8MetaItem;


pub type SpgAdminMd = SpuGroupMetadata<K8MetaItem>;
pub type SpgAdminStore = SpuGroupLocalStore<K8MetaItem>;
