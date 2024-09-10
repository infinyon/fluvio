use std::{
    collections::{HashSet, HashMap},
    ops::{Deref},
    sync::Arc,
};

use tracing::{warn, debug};
use tokio::sync::RwLock;

use fluvio_protocol::record::ReplicaKey;
use fluvio_types::{
    SpuId,
    event::offsets::{OffsetChangeListener, OffsetPublisher},
};

use crate::core::SpuLocalStore;

pub type SharedSpuUpdates = Arc<FollowerNotifier>;
pub type SharedSpuPendingUpdate = Arc<FollowerSpuPendingUpdates>;
#[derive(Debug)]
pub struct FollowerNotifier(RwLock<HashMap<SpuId, Arc<FollowerSpuPendingUpdates>>>);

impl Deref for FollowerNotifier {
    type Target = RwLock<HashMap<SpuId, Arc<FollowerSpuPendingUpdates>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FollowerNotifier {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(RwLock::new(HashMap::new())))
    }

    /// pending if exists
    pub async fn get(&self, spu: &SpuId) -> Option<Arc<FollowerSpuPendingUpdates>> {
        let read = self.read().await;
        read.get(spu).cloned()
    }

    /// update our self from current spu
    pub async fn sync_from_spus(&self, spus: &SpuLocalStore, local_spu: SpuId) {
        let mut writer = self.write().await;
        // remove non existent spu
        let keys: Vec<SpuId> = writer.keys().copied().collect();
        for spu in keys {
            if !spus.contains_key(&spu) {
                debug!(spu, "spu no longer valid,removing");
                writer.remove(&spu);
            }
        }
        // add new spu that doesn't exists
        for spu in spus.all_keys() {
            if spu != local_spu && !writer.contains_key(&spu) {
                debug!(spu, "spu pending doesn't exists, creating");
                let pending = FollowerSpuPendingUpdates {
                    event: Arc::new(OffsetPublisher::new(0)),
                    replicas: Arc::new(RwLock::new(HashSet::new())),
                };
                writer.insert(spu, Arc::new(pending));
            }
        }
    }

    /// notify followers that it's state need to be updated
    pub async fn notify_follower(&self, spu: &SpuId, replica: ReplicaKey) {
        let reader = self.read().await;
        if let Some(spu_ref) = reader.get(spu) {
            debug!(
                spu,
                %replica,
                "add notifier");
            spu_ref.add(replica).await;
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
        self.event.change_listener()
    }

    ///  add replica to be updated
    pub async fn add(&self, replica: ReplicaKey) {
        let mut write = self.replicas.write().await;
        write.insert(replica);
        self.event.update_increment();
    }

    /// drain all replicas
    pub async fn drain_replicas(&self) -> HashSet<ReplicaKey> {
        let mut write = self.replicas.write().await;
        write.drain().collect()
    }

    #[allow(unused)]
    pub async fn has_replica(&self, replica: &ReplicaKey) -> bool {
        let read = self.replicas.read().await;
        read.contains(replica)
    }
}
