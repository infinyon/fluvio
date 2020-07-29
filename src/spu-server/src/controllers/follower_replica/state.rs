use std::sync::RwLock;
use std::sync::Arc;
use std::fmt::Debug;
use std::collections::HashMap;
use std::collections::HashSet;

use log::debug;
use log::trace;
use log::error;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::channel;
use chashmap::CHashMap;
use chashmap::ReadGuard;
use chashmap::WriteGuard;

use flv_metadata_cluster::partition::ReplicaKey;
use kf_protocol::api::RecordSet;
use flv_storage::FileReplica;
use flv_storage::ConfigOption;
use flv_storage::StorageError;
use flv_storage::ReplicaStorage;
use flv_types::SpuId;
use flv_util::SimpleConcurrentBTreeMap;

use crate::core::storage::create_replica_storage;
use crate::controllers::leader_replica::UpdateOffsetRequest;
use crate::controllers::leader_replica::ReplicaOffsetRequest;
use super::FollowerReplicaControllerCommand;
use super::DefaultSyncRequest;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;

/// Maintains state for followers
/// Each follower controller maintains by SPU
#[derive(Debug)]
pub struct FollowersState<S> {
    mailboxes: SimpleConcurrentBTreeMap<SpuId, Sender<FollowerReplicaControllerCommand>>,
    replica_keys: RwLock<HashMap<SpuId, HashSet<ReplicaKey>>>, // replicas maintained by follower controller
    replicas: CHashMap<ReplicaKey, FollowerReplicaState<S>>,
}

impl<S> FollowersState<S> {
    pub fn new() -> Self {
        FollowersState {
            mailboxes: SimpleConcurrentBTreeMap::new(),
            replica_keys: RwLock::new(HashMap::new()),
            replicas: CHashMap::new(),
        }
    }

    pub fn new_shared() -> SharedFollowersState<S> {
        Arc::new(Self::new())
    }

    #[allow(dead_code)]
    pub fn followers_count(&self, leader: &SpuId) -> usize {
        let keys_lock = self.replica_keys.read().unwrap();
        if let Some(keys) = keys_lock.get(leader) {
            keys.len()
        } else {
            0
        }
    }

    pub fn has_replica(&self, key: &ReplicaKey) -> bool {
        self.replicas.contains_key(key)
    }

    pub fn get_replica(
        &self,
        key: &ReplicaKey,
    ) -> Option<ReadGuard<ReplicaKey, FollowerReplicaState<S>>> {
        self.replicas.get(key)
    }

    pub fn get_mut_replica(
        &self,
        key: &ReplicaKey,
    ) -> Option<WriteGuard<ReplicaKey, FollowerReplicaState<S>>> {
        self.replicas.get_mut(key)
    }

    /// remove followe replica
    /// if there are no more replicas per leader
    /// then we shutdown controller
    pub fn remove_replica(
        &self,
        leader: &SpuId,
        key: &ReplicaKey,
    ) -> Option<FollowerReplicaState<S>> {
        // remove replica from managed list for replica controller
        debug!("removing follower replica: {}, leader: {}", key, leader);
        let lock = self.mailboxes.read();
        drop(lock);
        let mut keys_lock = self.replica_keys.write().unwrap();
        let mut empty = false;
        if let Some(keys_by_spu) = keys_lock.get_mut(leader) {
            keys_by_spu.remove(key);
            if keys_by_spu.len() == 0 {
                empty = true;
            }
        } else {
            empty = true; // case where we dont find leader as well
        }

        let old_replica = self.replicas.remove(key);

        // if replica is empty then we need to terminate the leader
        if empty {
            debug!(
                "no more followers for follower controller: {}, terminating it",
                leader
            );
            if let Some(mut old_mailbox) = self.mailboxes.write().remove(leader) {
                debug!(
                    "removed mailbox for follower controller and closing it {}",
                    leader
                );
                old_mailbox.close_channel();
            } else {
                error!("there was no mailbox to close controller: {}", leader);
            }
        }

        old_replica
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
        if let Some(mut old_mailbox) = write_mailbox.insert(spu, sender.clone()) {
            debug!("there was old mailbox: {}, terminating it", spu);
            old_mailbox.close_channel();
        }
        (sender, receiver)
    }

    pub(crate) fn mailbox(&self, spu: &SpuId) -> Option<Sender<FollowerReplicaControllerCommand>> {
        self.mailboxes
            .read()
            .get(spu)
            .map(|mailbox| mailbox.clone())
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
        let mut keys_lock = self.replica_keys.write().unwrap();
        if let Some(keys_by_pus) = keys_lock.get_mut(&state.leader) {
            keys_by_pus.insert(state.replica.clone());
        } else {
            let mut keys = HashSet::new();
            keys.insert(state.replica.clone());
            keys_lock.insert(state.leader, keys);
        }

        self.replicas.insert(state.replica.clone(), state);
    }
}

impl FollowersState<FileReplica> {
    /// write records from leader to followe replica
    /// return updated offsets
    pub(crate) async fn send_records(&self, req: DefaultSyncRequest) -> UpdateOffsetRequest {
        let mut offsets = UpdateOffsetRequest::default();
        for topic_request in req.topics {
            let topic = &topic_request.name;
            for partition_request in topic_request.partitions {
                let rep_id = partition_request.partition_index;
                let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                trace!("sync request for replica: {}", replica_key);
                if let Some(mut replica) = self.get_mut_replica(&replica_key) {
                    match replica.send_records(partition_request.records).await {
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
        if let Some(replica) = self.get_replica(replica_id) {
            let storage = replica.storage();

            let mut replica_request = ReplicaOffsetRequest::default();
            replica_request.replica = replica_id.clone();
            replica_request.leo = storage.get_leo();
            replica_request.hw = storage.get_hw();
            offsets.replicas.push(replica_request);
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

    pub async fn send_records(&mut self, records: RecordSet) -> Result<(), StorageError> {
        trace!(
            "writing records to follower replica: {}, leader: {}",
            self.replica,
            self.leader
        );
        self.storage.send_records(records, false).await
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

        assert_eq!(states.followers_count(&10), 2);
        assert_eq!(states.followers_count(&20), 1);
        assert_eq!(states.followers_count(&30), 0);

        let old_state = states.remove_replica(&10, &k1).expect("old state exists");
        assert_eq!(old_state.leader, 10);
    }
}
