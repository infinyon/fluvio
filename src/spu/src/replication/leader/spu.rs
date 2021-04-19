use std::{collections::HashSet, ops::{Deref, DerefMut}, sync::Arc};

use event_listener::EventListener;
use tracing::{warn,debug};
use async_rwlock::RwLock;
use dashmap::DashMap;

use dataplane::ReplicaKey;
use fluvio_types::{SpuId, event::offsets::{OffsetChangeListener, OffsetPublisher}};

use crate::core::SpuLocalStore;

pub type SharedSpuUpdates = Arc<SpuUpdates>;
pub type SharedSpuPendingUpdate = Arc<FollowerSpuPendingUpdates>;
#[derive(Debug)]
pub struct SpuUpdates(DashMap<SpuId, Arc<FollowerSpuPendingUpdates>>);

impl Deref for SpuUpdates {
    type Target = DashMap<SpuId, SharedSpuPendingUpdate>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SpuUpdates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl SpuUpdates {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(DashMap::new()))
    }

    /// update our self from current spu
    pub fn sync_from_spus(&self, spus: &SpuLocalStore) {
        // remove non existent spu
        for map_ref in self.0.iter() {
            let spu = *map_ref.key();
            if !spus.contains_key(&spu) {
                debug!(spu, "spu no longer valid,removing");
                self.0.remove(&spu);
            }
        }
        // add new spu that doesn't exists
        for spu in spus.all_keys() {
            if !self.0.contains_key(&spu) {
                debug!(spu, "spu pending doesn't exists, creating");
                let pending = FollowerSpuPendingUpdates {
                    event: Arc::new(OffsetPublisher::new(0)),
                    replicas: Arc::new(RwLock::new(HashSet::new())),
                };
                self.0.insert(spu, Arc::new(pending));
            }
        }
    }

    /// replica's hw need be propogated to
    pub async fn update_hw(&self,spu: &SpuId,replica: ReplicaKey) {
        if let Some(spu_ref) = self.get(spu) {
            spu_ref.value().update_hw(replica).await;
        } else {
            warn!(spu,"invalid spu");
        }
    }

    
}

/// Sets of follower updates
#[derive(Debug)]
pub struct FollowerSpuPendingUpdates {
    event: Arc<OffsetPublisher>,
    replicas: Arc<RwLock<HashSet<ReplicaKey>>>,
}

impl FollowerSpuPendingUpdates {

    pub fn listener(&self) -> OffsetChangeListener {
        self.event.change_listner()
    }

    /// replica's hw need be propogated to
    pub async fn update_hw(&self,replica: ReplicaKey) {
        let mut write = self.replicas.write().await;
        write.insert(replica);
    }

    /// drain all replicas
    pub async fn dain_replicas(&self) -> HashSet<ReplicaKey> {
        let mut write = self.replicas.write().await;
        write.drain().collect()
    }
}
