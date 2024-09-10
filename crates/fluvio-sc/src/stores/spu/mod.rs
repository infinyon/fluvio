mod store;

pub use store::*;
pub use fluvio_controlplane_metadata::spu::*;
pub use fluvio_controlplane_metadata::store::k8::K8MetaItem;
pub use health_check::*;

pub type SpuAdminMd<C> = SpuMetadata<C>;
pub type SpuAdminStore = SpuLocalStore<K8MetaItem>;
pub type DefaultSpuStore = SpuLocalStore<u32>;

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
    use std::{collections::HashMap, ops::Deref, sync::Arc};

    use tracing::{instrument, debug, info};
    use tokio::sync::RwLock;

    use fluvio_types::{
        SpuId,
        event::offsets::{OffsetChangeListener, OffsetPublisher},
    };

    pub type SharedHealthCheck = Arc<HealthCheck>;

    /// Stores Current Health Check Data
    #[derive(Debug)]
    pub struct HealthCheck {
        health: RwLock<HashMap<SpuId, bool>>,
        event: Arc<OffsetPublisher>,
    }

    impl Deref for HealthCheck {
        type Target = RwLock<HashMap<SpuId, bool>>;

        fn deref(&self) -> &Self::Target {
            &self.health
        }
    }

    impl HealthCheck {
        pub fn shared() -> SharedHealthCheck {
            Arc::new(Self::new())
        }

        fn new() -> Self {
            Self {
                health: RwLock::new(HashMap::new()),
                event: OffsetPublisher::shared(0),
            }
        }

        pub fn listener(&self) -> OffsetChangeListener {
            self.event.change_listener()
        }

        /// update health check
        // TODO: Determine if we can follow the clippy suggestion w/o negatively affecting functionality
        #[allow(clippy::branches_sharing_code)]
        #[instrument(skip(self))]
        pub async fn update(&self, spu: SpuId, new_value: bool) {
            let mut write = self.health.write().await;
            if let Some(old_value) = write.insert(spu, new_value) {
                drop(write);
                if new_value != old_value {
                    info!(spu, new_value, "SPU health change");
                    self.event.update_increment();
                } else {
                    debug!(spu, new_value, "No SPU health change");
                }
            } else {
                drop(write);
                info!(spu, new_value, "Initial SPU health report");
                self.event.update_increment();
            }
        }
    }
}
