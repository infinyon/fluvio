use std::sync::Arc;
use std::fmt::Debug;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use tracing::{debug, trace, error, warn};
use tracing::instrument;
use async_channel::{Sender, Receiver, bounded as channel};
use async_rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use dashmap::DashMap;


use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use dataplane::record::RecordSet;
use dataplane::core::Encoder;
use fluvio_storage::{FileReplica, config::ConfigOption, StorageError, ReplicaStorage};
use fluvio_types::SpuId;
use fluvio_types::log_on_err;

use crate::core::{SharedGlobalContext, storage::create_replica_storage};
use crate::controllers::leader_replica::{UpdateOffsetRequest, ReplicaOffsetRequest};
use crate::controllers::follower_replica::ReplicaFollowerController;
use super::FollowerReplicaControllerCommand;
use super::sync::DefaultSyncRequest;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;

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
pub struct FollowersState<S>(DashMap<SpuId, SharedFollowersState<S>>);

impl<S> Default for FollowersState<S> {
    fn default() -> Self {
        FollowersState(DashMap::new())
    }
}

/*
    mailboxes: SimpleConcurrentBTreeMap<SpuId, Sender<FollowerReplicaControllerCommand>>,
    replica_keys: RwLock<HashMap<SpuId, HashSet<ReplicaKey>>>, // replicas maintained by follower controller
    replicas: CHashMap<ReplicaKey, FollowerReplicaState<S>>,
*/

impl<S> Deref for FollowersState<S> {
    type Target = DashMap<SpuId, SharedFollowersBySpu<S>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for FollowersState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S> FollowersState<S> {
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
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

impl<S> FollowersState<S>
where
    S: Debug,
{
    
}

impl FollowersState<FileReplica> {
    /// add replica, this creates FollowersSpu structure as necessary and starts up controller
    pub async fn add_replica(
        &self,
        ctx: SharedGlobalContext<FileReplica>,
        replica: Replica,
    ) {
        let leader = &replica.leader;
        debug!(%replica,"trying to adding follower replica");

        let followers_spu = self.0.get(leader).unwrap_or_else(||{
            debug!(
                "no existing follower controller exists for {},need to spin up",
                replica
            );

            let (followers_spu,receiver) = FollowersBySpu::new(leader);
          
            let follower_controller = ReplicaFollowerController::new(
                *leader,
                receiver,
                ctx.spu_localstore_owned(),
                followers_spu.clone(),
                ctx.config_owned(),
            );
            follower_controller.run();
            followers_spu
        });

        followers_spu.add_replica(replica.id);
        
    }

}

pub type SharedFollowersBySpu<S> = FollowersBySpu<S>;

/// list of followers by SPU
#[derive(Debug)]
pub struct FollowersBySpu<S> {
    spu: SpuId,
    replicas: RwLock<HashMap<ReplicaKey, FollowerReplicaState<S>>>,
    sender: Sender<FollowerReplicaControllerCommand>,
}

impl<S> FollowersBySpu<S> {

    pub fn shared(spu: SpuId) -> (Arc<Self>,Receiver<FollowerReplicaControllerCommand>) {
        let (sender, receiver) = channel(10);
        (Arc::new(Self {
            spu,
            replicas: RwLock::new(HashMap::new()),
            sender
        }),
        receiver)
    }

    pub fn sender(&self) -> &Sender<FollowerReplicaControllerCommand> {
        &self.sender
    }

    async fn read(&self) -> RwLockReadGuard<'_, HashMap<ReplicaKey, FollowerReplicaState<S>>> {
        self.replicas.read().await
    }

    async fn write(&self) -> RwLockWriteGuard<'_, HashMap<ReplicaKey, FollowerReplicaState<S>>> {
        self.replicas.write().await
    }

    /// insert new replica
    pub async fn insert_replica(&self, state: FollowerReplicaState<S>) {
        let mut write = self.write().await;
        write.insert(state.replica.clone(), state);
    }

    /// remove followe replica
    /// if there are no more replicas per leader
    /// then we shutdown controller
    #[instrument(skip(self))]
    pub fn remove_replica(
        &self,
        leader: &SpuId,
        key: &ReplicaKey,
    ) -> Option<FollowerReplicaState<S>> {
        if let Some(followers) = self.0.get(leader) {
            let write = followers.write();
            let replica = write.remove(key);
        } else {
            None
        }
    }

    
}

impl FollowersBySpu<FileReplica> {
    /// write records from leader to follower replica
    /// return updated offsets
    pub(crate) async fn write_topics(&self, mut req: DefaultSyncRequest) -> UpdateOffsetRequest {
        let mut offsets = UpdateOffsetRequest::default();
        let mut writer = self.write().await;
        for topic_request in &mut req.topics {
            let topic = &topic_request.name;
            for partition_request in &mut topic_request.partitions {
                let rep_id = partition_request.partition;
                let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                if let Some(replica) = writer.get_mut(&replica_key) {
                    match replica
                        .write_recordsets(&mut partition_request.records)
                        .await
                    {
                        Ok(valid_record) => {
                            if valid_record {
                                let follow_leo = replica.storage().get_leo();
                                let leader_hw = partition_request.hw;
                                debug!(follow_leo, leader_hw, "finish writing");
                                if follow_leo == leader_hw {
                                    debug!("follow leo and leader hw is same, updating hw");
                                    if let Err(err) =
                                        replica.mut_storage().update_high_watermark(leader_hw).await
                                    {
                                        error!("error writing replica high watermark: {}", err);
                                    }
                                }

                                offsets.replicas.push(replica.as_offset_request());
                            }
                        }
                        Err(err) => error!(
                            "problem writing replica: {}, error: {:#?}",
                            replica_key, err
                        ),
                    }
                } else {
                    error!(
                        "unable to find follower replica for writing: {}",
                        replica_key
                    );
                }
            }
        }
        offsets
    }

    /// offsets for all replicas
    pub(crate) async fn replica_offsets(&self, leader: &SpuId) -> UpdateOffsetRequest {
        let read = self.read().await;

        //let mut = UpdateOffsetRequest::default();

        let replicas = read
            .values()
            .map(|replica| replica.as_offset_request())
            .collect();

        UpdateOffsetRequest { replicas }
    }
}

/// State for Follower Replica Controller.
///
#[derive(Debug)]
pub struct FollowerReplicaState<S> {
    leader: SpuId,
    replica: ReplicaKey,
    storage: S,
}

impl<S> FollowerReplicaState<S> {
    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn mut_storage(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn storage_owned(self) -> S {
        self.storage
    }
}

impl FollowerReplicaState<FileReplica> {
    pub async fn new<'a>(
        local_spu: SpuId,
        leader: SpuId,
        replica: &'a ReplicaKey,
        config: &'a ConfigOption,
    ) -> Result<Self, StorageError> {
        debug!(
            "creating follower replica: local_spu: {}, replica: {}, leader: {}, base_dir: {}",
            local_spu,
            replica,
            leader,
            config.base_dir.display()
        );

        let storage = create_replica_storage(local_spu, replica, &config).await?;

        Ok(Self {
            leader,
            replica: replica.clone(),
            storage,
        })
    }

    /// write records
    /// if true, records's base offset matches,
    ///    false,invalid record sets has been sent
    pub async fn write_recordsets(
        &mut self,
        records: &mut RecordSet,
    ) -> Result<bool, StorageError> {
        debug!(
            replica = %self.replica,
            records = records.total_records(),
            size = records.write_size(0),
            base_offset = records.base_offset(),
            "writing records"
        );

        // check to ensure base offset should be same as leo
        if records.base_offset() != self.storage.get_leo() {
            warn!(
                storage_leo = self.storage.get_leo(),
                "storage leo is not same as base offset"
            );
            Ok(false)
        } else {
            self.storage.write_recordset(records, false).await?;
            Ok(true)
        }
    }

    pub async fn remove(self) -> Result<(), StorageError> {
        self.storage.remove().await
    }

    /// add replica offsets to request
    fn as_offset_request(&self) -> ReplicaOffsetRequest {
        let storage = self.storage();
        ReplicaOffsetRequest {
            replica: self.replica.clone(),
            leo: storage.get_leo(),
            hw: storage.get_hw(),
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
