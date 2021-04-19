use std::{collections::HashSet, sync::Arc};

use tracing::debug;
use async_rwlock::RwLock;
use dashmap::DashMap;

use dataplane::ReplicaKey;
use fluvio_types::{SpuId, event::offsets::OffsetPublisher};

use crate::core::SpuLocalStore;

pub type SharedSpuUpdates = Arc<SpuUpdates>;
#[derive(Debug)]
pub struct SpuUpdates(DashMap<SpuId, Arc<FollowerSpuPendingUpdates>>);

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
                    event: OffsetPublisher::new(0),
                    replicas: Arc::new(RwLock::new(HashSet::new())),
                };
                self.0.insert(spu, Arc::new(pending));
            }
        }
    }
}

/// Sets of follower updates
#[derive(Debug)]
pub struct FollowerSpuPendingUpdates {
    event: OffsetPublisher,
    replicas: Arc<RwLock<HashSet<ReplicaKey>>>,
}
