use std::sync::RwLock;
use std::sync::Arc;
use std::fmt::Debug;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::{Deref,DerefMut};

use tracing::{debug,trace,error};
use tracing::instrument;
use async_channel::{Sender,Receiver,bounded as channel};
use dashmap::DashMap;

use flv_util::SimpleConcurrentBTreeMap;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use dataplane::record::RecordSet;
use fluvio_storage::{ FileReplica,config::ConfigOption,StorageError,ReplicaStorage,OffsetUpdate};
use fluvio_types::SpuId;

use crate::core::storage::create_replica_storage;
use crate::controllers::leader_replica::{UpdateOffsetRequest,ReplicaOffsetRequest};
use super::FollowerReplicaControllerCommand;
use super::DefaultSyncRequest;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;

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
        keys_lock.get(leader).map(|keys| keys.len()).unwrap_or_default()
    }

    /// remove keys by SPU, tru if empty
    fn remove_replica_keys(
        &self,
        leader: &SpuId,
        key: &ReplicaKey,
    ) -> bool {
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
    pub fn insert_replica(&self, leader: SpuId,replica: ReplicaKey) {
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


/// Maintains state for followers
/// Each follower controller maintains by SPU
#[derive(Debug)]
pub struct FollowersState<S> {
    replicas: DashMap<ReplicaKey,FollowerReplicaState<S>>,
    replica_keys: ReplicaKeys,    // list of replica key mapping for each spu
    mailboxes: SimpleConcurrentBTreeMap<SpuId, Sender<FollowerReplicaControllerCommand>>
}

/* 
    mailboxes: SimpleConcurrentBTreeMap<SpuId, Sender<FollowerReplicaControllerCommand>>,
    replica_keys: RwLock<HashMap<SpuId, HashSet<ReplicaKey>>>, // replicas maintained by follower controller
    replicas: CHashMap<ReplicaKey, FollowerReplicaState<S>>,
*/

impl<S> Deref for FollowersState<S> {
    type Target = DashMap<ReplicaKey, FollowerReplicaState<S>>;

    fn deref(&self) -> &Self::Target {
        &self.replicas
    }
}

impl<S> DerefMut for FollowersState<S>  {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.replicas
    }
}



impl<S> FollowersState<S> {
    pub fn new() -> Self {
        Self {
            replicas: DashMap::new(),
            replica_keys: ReplicaKeys::new(),
            mailboxes: SimpleConcurrentBTreeMap::new(),
        }
    }

    pub fn new_shared() -> SharedFollowersState<S> {
        Arc::new(Self::new())
    }

    
    pub fn has_replica(&self, key: &ReplicaKey) -> bool {
        self.contains_key(key)
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
        
        let no_keys = self.replica_keys.remove_replica_keys(leader, key);
        
        let old_replica = self.replicas.remove(key);

        // if no more keys 
        if no_keys {
            debug!(
                "no more followers for follower controller: {}, terminating it",
                leader
            );
            if let Some(old_mailbox) = self.mailboxes.write().remove(leader) {
                debug!(
                    "removed mailbox for follower controller and closing it {}",
                    leader
                );
                old_mailbox.close();
            } else {
                error!("there was no mailbox to close controller: {}", leader);
            }
        }

        old_replica.map(|(_,state)| state)
    }

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
}

impl<S> FollowersState<S>
where
    S: Debug,
{
    /// insert new replica, once replica has been insert, need to update the leader
    /// this is called by follower controller
    pub fn insert_replica(&self, state: FollowerReplicaState<S>) {
        trace!("inserting new follower replica: {:#?}", state);

        self.replica_keys.insert_replica(state.leader,state.replica.clone());
        self.replicas.insert(state.replica.clone(), state);
    }
}

impl FollowersState<FileReplica> {

    /// write records from leader to follower replica
    /// return updated offsets
    pub(crate) async fn send_records(&self, mut req: DefaultSyncRequest) -> UpdateOffsetRequest {
        let mut offsets = UpdateOffsetRequest::default();
        for topic_request in &mut req.topics {
            let topic = &topic_request.name;
            for partition_request in &mut topic_request.partitions {
                let rep_id = partition_request.partition_index;
                let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                trace!("sync request for replica: {}", replica_key);
                if let Some(mut replica) = self.get_mut(&replica_key) {
                    match replica
                        .write_recordsets(&mut partition_request.records)
                        .await
                    {
                        Ok(_) => {
                            trace!(
                                "successfully written send to follower replica: {}",
                                replica_key
                            );
                            let end_offset = replica.storage().get_leo();
                            let high_watermark = partition_request.high_watermark;
                            if end_offset == high_watermark {
                                trace!("follower replica: {} end offset is same as leader highwater, updating ",end_offset);
                                if let Err(err) = replica
                                    .mut_storage()
                                    .update_high_watermark(high_watermark)
                                    .await
                                {
                                    error!("error writing replica high watermark: {}", err);
                                }
                            } else {
                                trace!("replica: {} high watermark is not same as leader high watermark: {}",replica_key,end_offset);
                            }
                            drop(replica);
                            self.add_replica_offset_to(&replica_key, &mut offsets);
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
    pub(crate) fn replica_offsets(&self, leader: &SpuId) -> UpdateOffsetRequest {
        let replica_indexes = self.replica_keys.read().unwrap();

        let mut offsets = UpdateOffsetRequest::default();

        if let Some(keys) = replica_indexes.get(leader) {
            for replica_id in keys {
                self.add_replica_offset_to(replica_id, &mut offsets);
            }
        }
        offsets
    }

    fn add_replica_offset_to(&self, replica_id: &ReplicaKey, offsets: &mut UpdateOffsetRequest) {
        if let Some(replica) = self.get(replica_id) {
            let storage = replica.storage();

            let replica_request = ReplicaOffsetRequest {
                replica: replica_id.clone(),
                leo: storage.get_leo(),
                hw: storage.get_hw(),
            };
            offsets.replicas.push(replica_request)
        } else {
            error!(
                "no follow replica found: {}, should not be possible",
                replica_id,
            );
        }
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

    #[instrument()]
    pub async fn write_recordsets(&mut self, records: &mut RecordSet) -> Result<OffsetUpdate, StorageError> {
        self.storage.write_recordset(records,false).await
    }

    pub async fn remove(self) -> Result<(), StorageError> {
        self.storage.remove().await
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
