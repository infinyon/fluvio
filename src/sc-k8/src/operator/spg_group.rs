use k8_metadata::core::metadata::K8Obj;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::spg::SpuGroupStatus;
use types::SpuId;

use flv_sc_core::core::spus::SpuLocalStore;

pub type SpuGroupObj = K8Obj<SpuGroupSpec, SpuGroupStatus>;

/// need for adding SPG extensions
pub trait SpuValidation {
    fn is_already_valid(&self) -> bool;
    fn is_conflict_with(&self, spu_store: &SpuLocalStore) -> Option<SpuId>;
}

impl SpuValidation for SpuGroupObj {
    /// check if I am already been validated
    fn is_already_valid(&self) -> bool {
        self.status
            .as_ref()
            .map(|status| status.is_already_valid())
            .unwrap_or(false)
    }

    /// check if my group's id is conflict with my spu local store
    fn is_conflict_with(&self, spu_store: &SpuLocalStore) -> Option<SpuId> {
        if self.is_already_valid() {
            return None;
        }

        let min_id = self.spec.min_id() as SpuId;

        spu_store.is_conflict(
            &self.metadata.uid,
            min_id,
            min_id + self.spec.replicas as SpuId,
        )
    }
}
