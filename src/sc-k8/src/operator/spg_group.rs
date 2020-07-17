
use async_trait::async_trait;
use flv_metadata::k8::metadata::K8Obj;
use flv_metadata::spg::K8SpuGroupSpec;
use flv_types::SpuId;

use flv_sc_core::stores::spu::*;

pub type SpuGroupObj = K8Obj<K8SpuGroupSpec>;

/// need for adding SPG extensions
#[async_trait]
pub trait SpuValidation {
    fn is_already_valid(&self) -> bool;
    async fn is_conflict_with(&self, spu_store: &SpuAdminStore) -> Option<SpuId>;
}

#[async_trait]
impl SpuValidation for SpuGroupObj {
    /// check if I am already been validated
    fn is_already_valid(&self) -> bool {
        self.status.is_already_valid()
    }

    /// check if my group's id is conflict with my spu local store
    async fn is_conflict_with(&self, spu_store: &SpuAdminStore) -> Option<SpuId> {
        if self.is_already_valid() {
            return None;
        }

        let min_id = self.spec.min_id as SpuId;

        is_conflict(
            spu_store,
            &self.metadata.uid,
            min_id,
            min_id + self.spec.replicas as SpuId,
        ).await
    }
}
