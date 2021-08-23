pub use fluvio_controlplane_metadata::spu::store::*;
pub use fluvio_controlplane_metadata::spu::*;
pub use fluvio_controlplane_metadata::store::k8::K8MetaItem;

pub type SpuAdminMd = SpuMetadata<K8MetaItem>;
pub type SpuAdminStore = SpuLocalStore<K8MetaItem>;

// check if given range is conflict with any of the range
pub async fn is_conflict(
    spu_store: &SpuAdminStore,
    owner_uid: String,
    start: i32,
    end_exclusive: i32,
) -> Option<i32> {
    for (_, spu) in spu_store.read().await.iter() {
        if !spu.is_owned(&owner_uid) {
            let id = spu.spec.id;
            if id >= start && id < end_exclusive {
                return Some(id);
            }
        }
    }
    None
}


mod health_check {
    use std::{collections::HashMap, ops::Deref};

    use async_lock::RwLock;
    use fluvio_types::SpuId;

    /// Stores Curret Health Check Data
    pub struct HealthCheck(RwLock<HashMap<SpuId, bool>>);

    impl Deref for HealthCheck {
        type Target = RwLock<HashMap<SpuId,bool>>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    


}
