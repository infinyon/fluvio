pub use fluvio_controlplane_metadata::spu::store::*;
pub use fluvio_controlplane_metadata::spu::*;
pub use fluvio_controlplane_metadata::store::k8::K8MetaItem;
pub use health_check::*;

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
    use std::{collections::HashMap,  sync::Arc};

    use async_lock::RwLock;
    use fluvio_types::{SpuId, event::offsets::{OffsetChangeListener, OffsetPublisher}};

    /// Stores Curret Health Check Data
    #[derive(Debug)]
    pub struct HealthCheck {
        health: RwLock<HashMap<SpuId, bool>>,
        event: Arc<OffsetPublisher>
    }



    impl HealthCheck {

        /// get current status
        pub async fn status(&self, spu: &SpuId) -> HashMap<SpuId,bool> {
            let read = self.health.read().await;
            read.clone()
        }
        
        pub fn listener(&self) -> OffsetChangeListener {
            self.event.change_listner()
        }
            
        /// update health check
        pub async fn update(&self,spu: SpuId,value: bool)  {

            let mut write = self.health.write().await;
            write.insert(spu,value);
            drop(write);

            self.event.update_increment();
        }

    }


}
