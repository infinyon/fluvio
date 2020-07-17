
pub use flv_metadata::spg::*;
pub use flv_metadata::spg::store::*;

use super::*;

/// for group status, we have custom spu group spec
impl K8ExtendedSpec for SpuGroupSpec {
    type K8Spec  = K8SpuGroupSpec;
    type K8Status = Self::Status;
}

pub type  SpgAdminMd = SpuGroupMetadata<K8MetaItem>;
pub type  SpgAdminStore = SpuGroupLocalStore<K8MetaItem>;