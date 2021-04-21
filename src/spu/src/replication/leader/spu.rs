use std::{
    collections::{HashSet, HashMap},
    ops::{Deref},
    sync::Arc,
};

use tracing::{warn, debug};
use async_rwlock::RwLock;

use dataplane::ReplicaKey;
use fluvio_types::{
    SpuId,
    event::offsets::{OffsetChangeListener, OffsetPublisher},
};

use crate::core::SpuLocalStore;

pub type SharedSpuUpdates = Arc<SpuUpdates>;
pub type SharedSpuPendingUpdate = Arc<FollowerSpuPendingUpdates>;
#[derive(Debug)]
pub struct SpuUpdates(RwLock<HashMap<SpuId, Arc<FollowerSpuPendingUpdates>>>);

impl Deref for SpuUpdates {
    type Target = RwLock<HashMap<SpuId, Arc<FollowerSpuPendingUpdates>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SpuUpdates {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(RwLock::new(HashMap::new())))
    }

    /// pending if exists
    pub async fn get(&self, spu: &SpuId) -> Option<Arc<FollowerSpuPendingUpdates>> {
        let read = self.read().await;
        read.get(spu).map(|value| value.clone())
    }

    /// update our self from current spu
    pub async fn sync_from_spus(&self, spus: &SpuLocalStore) {
        let mut writer = self.write().await;
        // remove non existent spu
        let keys: Vec<SpuId> = writer.keys().map(|k| *k).collect();
        for spu in keys {
            if !spus.contains_key(&spu) {
                debug!(spu, "spu no longer valid,removing");
                writer.remove(&spu);
            }
        }
        // add new spu that doesn't exists
        for spu in spus.all_keys() {
            if !writer.contains_key(&spu) {
                debug!(spu, "spu pending doesn't exists, creating");
                let pending = FollowerSpuPendingUpdates {
                    event: Arc::new(OffsetPublisher::new(0)),
                    replicas: Arc::new(RwLock::new(HashSet::new())),
                };
                writer.insert(spu, Arc::new(pending));
            }
        }
    }

    /// replica's hw need be propogated to
    pub async fn update_hw(&self, spu: &SpuId, replica: ReplicaKey) {
        if let Some(spu_ref) = self.get(spu).await {
            spu_ref.update_hw(replica).await;
        } else {
            warn!(spu, "invalid spu");
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
    pub async fn update_hw(&self, replica: ReplicaKey) {
        let mut write = self.replicas.write().await;
        write.insert(replica);
        self.event.update_increment();
    }

    /// drain all replicas
    pub async fn dain_replicas(&self) -> HashSet<ReplicaKey> {
        let mut write = self.replicas.write().await;
        write.drain().collect()
    }
}
