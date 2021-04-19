
use std::{collections::HashSet, sync::Arc};

use async_rwlock::RwLock;
use dashmap::DashMap;

use dataplane::ReplicaKey;
use fluvio_types::{SpuId, event::offsets::OffsetPublisher};

use crate::core::SpuLocalStore;



pub struct SharedSpuUpdates(DashMap<SpuId,Arc<FollowerSpuPendingUpdates>>);

impl SharedSpuUpdates {

    /// update our self from current spu
    pub fn sync_from_spus(spu: &SpuLocalStore) {
        // remove non existent spu
        
    }
}


/// rlist of follower updates
pub struct FollowerSpuPendingUpdates {
    event: OffsetPublisher,
    replicas: Arc<RwLock<HashSet<ReplicaKey>>>,
}