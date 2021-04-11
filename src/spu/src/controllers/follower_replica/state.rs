use std::sync::Arc;
use std::fmt::Debug;

use std::ops::{Deref, DerefMut};

use tracing::{debug, warn};
use async_rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use dashmap::DashMap;

use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use dataplane::record::RecordSet;
use dataplane::core::Encoder;
use dataplane::{Offset};
use fluvio_storage::{FileReplica, StorageError, ReplicaStorage};
use fluvio_types::SpuId;
use fluvio_types::event::offsets::OffsetPublisher;

use crate::{
    config::SpuConfig,
    core::{storage::create_replica_storage},
};
use crate::controllers::leader_replica::ReplicaOffsetRequest;
use crate::controllers::follower_replica::ReplicaFollowerController;
use crate::config::Log;
use crate::core::DefaultSharedGlobalContext;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;
pub type SharedFollowerReplicaState<S> = Arc<FollowerReplicaState<S>>;
pub type SharedFollowersBySpu = Arc<FollowersBySpu>;

/*
/// maintain mapping of replicas for each SPU
#[derive(Debug)]
pub struct ReplicaKeys(RwLock<HashMap<SpuId, HashSet<ReplicaKey>>>);

impl ReplicaKeys {
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    #[allow(dead_code)]
    pub fn followers_count(&self, leader: &SpuId) -> usize {
        let keys_lock = self.read().unwrap();
        keys_lock
            .get(leader)
            .map(|keys| keys.len())
            .unwrap_or_default()
    }

    /// remove keys by SPU, tru if empty
    fn remove_replica_keys(&self, leader: &SpuId, key: &ReplicaKey) -> bool {
        let mut keys_lock = self.write().unwrap();
        if let Some(keys_by_spu) = keys_lock.get_mut(leader) {
            keys_by_spu.remove(key);
            keys_by_spu.is_empty()
        } else {
            true
        }
    }

    /// insert new replica, once replica has been insert, need to update the leader
    /// this is called by follower controller
    pub fn insert_replica(&self, leader: SpuId, replica: ReplicaKey) {
        debug!( leader, %replica, "inserting new follower replica");
        let mut keys_lock = self.write().unwrap();
        if let Some(keys_by_pus) = keys_lock.get_mut(&leader) {
            keys_by_pus.insert(replica);
        } else {
            let mut keys = HashSet::new();
            keys.insert(replica);
            keys_lock.insert(leader, keys);
        }
    }
}

impl Deref for ReplicaKeys {
    type Target = RwLock<HashMap<SpuId, HashSet<ReplicaKey>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReplicaKeys {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

*/

/// Maintains state for followers
/// Each follower controller maintains by SPU
#[derive(Debug)]
pub struct FollowersState<S> {
    states: DashMap<ReplicaKey, SharedFollowerReplicaState<S>>,
    leaders: DashMap<SpuId, FollowersBySpu>,
}

impl<S> Deref for FollowersState<S> {
    type Target = DashMap<ReplicaKey, SharedFollowerReplicaState<S>>;

    fn deref(&self) -> &Self::Target {
        &self.states
    }
}

impl<S> DerefMut for FollowersState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.states
    }
}

impl<S> FollowersState<S> {
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self {
            states: DashMap::new(),
            leaders: DashMap::new(),
        })
    }

    /*
    pub fn has_replica(&self, key: &ReplicaKey) -> bool {
        self.contains_key(key)
    }
    */

    /*
    /// insert new mailbox and return receiver
    /// this is called by sc dispatcher
    pub(crate) fn insert_mailbox(
        &self,
        spu: SpuId,
    ) -> (
        Sender<FollowerReplicaControllerCommand>,
        Receiver<FollowerReplicaControllerCommand>,
    ) {
        let (sender, receiver) = channel(10);
        let mut write_mailbox = self.mailboxes.write();
        debug!("inserting mailbox for follower controller: {}", spu);
        if let Some(old_mailbox) = write_mailbox.insert(spu, sender.clone()) {
            debug!("there was old mailbox: {}, terminating it", spu);
            old_mailbox.close();
        }
        (sender, receiver)
    }

    pub(crate) fn mailbox(&self, spu: &SpuId) -> Option<Sender<FollowerReplicaControllerCommand>> {
        self.mailboxes.read().get(spu).cloned()
    }
    */
}

impl FollowersState<FileReplica> {
    async fn new_replica(
        &self,
        leader: SpuId,
        id: ReplicaKey,
        config: &SpuConfig,
    ) -> Result<Arc<FollowerReplicaState<FileReplica>>, StorageError> {
        Ok(Arc::new(
            FollowerReplicaState::new(config.id(), leader, id, &config.log).await?,
        ))
    }

    /// try to add new replica
    /// if there isn't existing spu group, create new one and return new replica
    /// otherwise check if there is existing state, if exists return none otherwise create new
    pub async fn add_replica(
        self: Arc<Self>,
        replica: Replica,
        ctx: DefaultSharedGlobalContext,
    ) -> Result<Option<SharedFollowerReplicaState<FileReplica>>, StorageError> {
        let leader = replica.leader;
        let config = ctx.config_owned();

        if let Some(followers) = self.states.get(&replica.id) {
            // follower exists, nothing to do
            warn!(%replica,"replica already exists");
            Ok(None)
        } else {
            debug!(
                "no existing follower controller exists for {},need to spin up",
                replica
            );

            let replica_state = self
                .new_replica(leader, replica.id.clone(), &config)
                .await?;
            self.states.insert(replica.id, replica_state.clone());

            // check if we have controllers
            if let Some(leaders) = self.leaders.get(&leader) {
                leaders.sync();
            } else {
                // don't have leader, so we need to create
                let followers_spu = FollowersBySpu::shared(leader);
                ReplicaFollowerController::run(
                    leader,
                    ctx.spu_localstore_owned(),
                    self.clone(),
                    followers_spu.clone(),
                    config,
                );
            }

            Ok(Some(replica_state))
        }
    }

    pub async fn remove_replica(&self, replica_id: &ReplicaKey) {}
}

/// list of followers by SPU
#[derive(Debug)]
pub struct FollowersBySpu {
    spu: SpuId,
    pub events: Arc<OffsetPublisher>,
}

impl FollowersBySpu {
    pub fn shared(spu: SpuId) -> Arc<Self> {
        Arc::new(Self {
            spu,
            events: Arc::new(OffsetPublisher::new(0)),
        })
    }

    /// create clone of sync events
    fn clone_sync_events(&self) -> Arc<OffsetPublisher> {
        self.events.clone()
    }

    /// notify controller to indicate need to re-sync
    pub fn sync(&self) {
        let last_value = self.events.current_value();
        self.events.update(last_value + 1);
    }
}

/// State for Follower Replica Controller.
///
#[derive(Debug)]
pub struct FollowerReplicaState<S> {
    leader: SpuId,
    replica: ReplicaKey,
    storage: RwLock<S>,
    leo: Arc<OffsetPublisher>,
    hw: Arc<OffsetPublisher>,
}

impl<S> FollowerReplicaState<S> {
    pub fn leader(&self) -> SpuId {
        self.leader
    }

    /// readable ref to storage
    async fn storage(&self) -> RwLockReadGuard<'_, S> {
        self.storage.read().await
    }

    /// writable ref to storage
    async fn mut_storage(&self) -> RwLockWriteGuard<'_, S> {
        self.storage.write().await
    }
}

impl FollowerReplicaState<FileReplica> {
    pub async fn new(
        local_spu: SpuId,
        leader: SpuId,
        replica: ReplicaKey,
        config: &Log,
    ) -> Result<Self, StorageError> {
        debug!(
            "creating follower replica: local_spu: {}, replica: {}, leader: {}, base_dir: {}",
            local_spu,
            replica,
            leader,
            config.base_dir.display()
        );

        let storage = create_replica_storage(local_spu, &replica, config).await?;
        let leo = Arc::new(OffsetPublisher::new(storage.get_leo()));
        let hw = Arc::new(OffsetPublisher::new(storage.get_hw()));

        Ok(Self {
            leader,
            replica,
            storage: RwLock::new(storage),
            leo,
            hw,
        })
    }

    pub fn leo(&self) -> Offset {
        self.leo.current_value()
    }

    pub fn hw(&self) -> Offset {
        self.hw.current_value()
    }

    /// write records
    /// if true, records's base offset matches,
    ///    false,invalid record sets has been sent
    pub async fn write_recordsets(&self, records: &mut RecordSet) -> Result<bool, StorageError> {
        debug!(
            replica = %self.replica,
            records = records.total_records(),
            size = records.write_size(0),
            base_offset = records.base_offset(),
            "writing records"
        );

        let mut writable = self.mut_storage().await;

        // check to ensure base offset should be same as leo
        let storage_leo = writable.get_leo();
        if records.base_offset() != storage_leo {
            warn!(storage_leo, "storage leo is not same as base offset");
            Ok(false)
        } else {
            writable.write_recordset(records, false).await?;
            // update our leo
            let leo = writable.get_leo();
            debug!(leo, "updated leo");
            self.leo.update(leo);
            Ok(true)
        }
    }

    pub async fn update_high_watermark(&self, hw: Offset) -> Result<bool, StorageError> {
        let mut writable = self.mut_storage().await;
        writable.update_high_watermark(hw).await
    }

    /// remove storage
    pub async fn remove(self) -> Result<(), StorageError> {
        self.mut_storage().await.remove().await
    }

    /// convert to offset request
    pub fn as_offset_request(&self) -> ReplicaOffsetRequest {
        ReplicaOffsetRequest {
            replica: self.replica.clone(),
            leo: self.leo(),
            hw: self.hw(),
        }
    }
}

#[cfg(test)]
mod test {

    use super::FollowerReplicaState;
    use super::FollowersState;

    #[derive(Debug)]
    struct FakeStorage {}

    #[test]
    fn test_insert_and_remove_state() {
        let f1 = FollowerReplicaState {
            leader: 10,
            replica: ("topic", 0).into(),
            storage: FakeStorage {},
        };

        let k1 = f1.replica.clone();

        let f2 = FollowerReplicaState {
            leader: 20,
            replica: ("topic2", 0).into(),
            storage: FakeStorage {},
        };

        let f3 = FollowerReplicaState {
            leader: 10,
            replica: ("topic", 1).into(),
            storage: FakeStorage {},
        };

        let states = FollowersState::new();
        states.insert_replica(f1);
        states.insert_replica(f2);
        states.insert_replica(f3);

        assert_eq!(states.replica_keys.followers_count(&10), 2);
        assert_eq!(states.replica_keys.followers_count(&20), 1);
        assert_eq!(states.replica_keys.followers_count(&30), 0);

        let old_state = states.remove_replica(&10, &k1).expect("old state exists");
        assert_eq!(old_state.leader, 10);
    }
}
