use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use std::fmt;

use tracing::debug;
use tracing::trace;
use tracing::error;
use tracing::warn;
use async_rwlock::{RwLock};
use async_channel::{Sender, SendError};

use fluvio_socket::SinkPool;
use dataplane::record::RecordSet;
use dataplane::{Offset, Isolation};
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::partition::{ReplicaKey, Replica};
use fluvio_controlplane::LrsRequest;
use fluvio_storage::{
    FileReplica, config::ConfigOption, StorageError, OffsetUpdate, SlicePartitionResponse,
    ReplicaStorage,
};
use fluvio_types::SpuId;
use fluvio_types::event::offsets::OffsetPublisher;

use crate::{controllers::sc::SharedSinkMessageChannel};
use crate::core::storage::{create_replica_storage};
use crate::controllers::follower_replica::{
    FileSyncRequest, PeerFileTopicResponse, PeerFilePartitionResponse,
};

pub type SharedLeaderState<S> = Arc<LeaderReplicaState<S>>;
pub type SharedFileLeaderState = Arc<LeaderReplicaState<FileReplica>>;

use super::LeaderReplicaControllerCommand;

#[derive(Debug)]
pub struct LeaderReplicaState<S> {
    replica_id: ReplicaKey,
    leader_id: SpuId,
    storage: RwLock<S>,
    followers: RwLock<BTreeMap<SpuId, FollowerReplicaInfo>>,
    leo: OffsetPublisher,
    hw: OffsetPublisher,
    commands: Sender<LeaderReplicaControllerCommand>,
}

impl<S> fmt::Display for LeaderReplicaState<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Leader {}", self.replica_id)
    }
}

impl<S> LeaderReplicaState<S>
where
    S: ReplicaStorage,
{
    pub fn new<R>(
        replica_id: R,
        leader_id: SpuId,
        storage: S,
        follower_ids: HashSet<SpuId>,
        commands: Sender<LeaderReplicaControllerCommand>,
    ) -> Self
    where
        R: Into<ReplicaKey>,
    {
        let leo = OffsetPublisher::new(storage.get_hw());
        let hw = OffsetPublisher::new(storage.get_leo());
        let followers = FollowerReplicaInfo::ids_to_map(leader_id, follower_ids);
        Self {
            replica_id: replica_id.into(),
            leader_id,
            followers: RwLock::new(followers),
            storage: RwLock::new(storage),
            leo,
            hw,
            commands,
        }
    }

    pub fn replica_id(&self) -> &ReplicaKey {
        &self.replica_id
    }

    /// log end offset
    pub fn leo(&self) -> Offset {
        self.leo.current_value()
    }

    /// high watermark
    pub fn hw(&self) -> Offset {
        self.hw.current_value()
    }


    // probably only used in the test
    /*
    #[allow(dead_code)]
    pub(crate) fn followers(&self, spu: &SpuId) -> Option<FollowerReplicaInfo> {
        self.followers.read().await.get(spu).cloned()
    }
    */

    /// send message to leader controller
    pub async fn send_message(
        &self,
        command: LeaderReplicaControllerCommand,
    ) -> Result<(), SendError<LeaderReplicaControllerCommand>> {
        self.commands.send(command).await
    }

    /// update followers offset, return (status_needs_to_changed,follower to be synced)
    ///
    /// // case 1:  follower offset has same value as leader
    /// //          leader: leo: 2, hw: 2,  follower: leo: 2, hw: 2
    /// //          Input: leo 2, hw: 2,  this happens during follower resync.
    /// //          Expect:  no changes,
    ///
    /// // case 2:  follower offset is same as previous
    /// //          leader: leo: 2, hw: 2,  follower: leo: 1, hw: 1
    /// //          Input:  leo: 1, hw:1,  
    /// //          Expect, no status but follower sync
    /// //
    /// // case 3:  different follower offset
    /// //          leader: leo: 3, hw: 3,  follower: leo: 1, hw: 1
    /// //          Input:  leo: 2, hw: 2,
    /// //          Expect, status change, follower sync  
    pub async fn update_follower_offsets<F>(&self, offset: F) -> (bool, Option<FollowerReplicaInfo>)
    where
        F: Into<FollowerOffsetUpdate>,
    {
        let follower_offset = offset.into();
        // if update offset is greater than leader than something is wrong, in this case
        // we truncate the the follower offset
        let follower_id = follower_offset.follower_id;
        let mut follower_info = FollowerReplicaInfo::new(follower_offset.leo, follower_offset.hw);

        let leader_leo = self.leo();
        let leader_hw = self.hw();

        if follower_info.leo > leader_leo {
            warn!(
                "offset leo: {} is greater than leader leo{} ",
                follower_info.leo, leader_leo
            );
            follower_info.leo = leader_leo;
        }

        let mut followers = self.followers.write().await;

        let changed = if let Some(old_info) = followers.insert(follower_id, follower_info.clone()) {
            old_info != follower_info
        } else {
            false
        };

        (
            changed,
            if leader_leo != follower_info.leo || leader_hw != follower_info.hw {
                Some(follower_info)
            } else {
                None
            },
        )
    }

    /// compute list of followers that need to be sync
    /// this is done by checking diff of end offset and high watermark
    async fn need_follower_updates(&self) -> Vec<(SpuId, FollowerReplicaInfo)> {
        let leo = self.leo();
        let hw = self.hw();

        trace!("computing follower offset for leader: {}, replica: {}, end offset: {}, high watermarkK {}",self.leader_id,self.replica_id,leo,hw);

        let reader = self.followers.read().await;
        reader
            .iter()
            .filter(|(_, follower_info)| {
                follower_info.is_valid() && !follower_info.is_same(hw, leo)
            })
            .map(|(follower_id, follower_info)| {
                debug!(
                    "replica: {}, follower: {} needs to be updated",
                    self.replica_id, follower_id
                );
                trace!(
                    "follow: {} has different hw: {:#?}",
                    follower_id,
                    follower_info
                );
                (*follower_id, follower_info.clone())
            })
            .collect()
    }

    /// convert myself as
    async fn as_lrs_request(&self) -> LrsRequest {
        let leader = (self.leader_id, self.hw(), self.leo()).into();
        let replicas = self
            .followers
            .read()
            .await
            .iter()
            .map(|(follower_id, follower_info)| {
                (*follower_id, follower_info.hw(), follower_info.leo()).into()
            })
            .collect();

        LrsRequest::new(self.replica_id.clone(), leader, replicas)
    }

    pub async fn send_status_to_sc(&self, sc_sink: &SharedSinkMessageChannel) {
        let lrs = self.as_lrs_request().await;
        debug!(hw = lrs.leader.hw, leo = lrs.leader.leo);
        sc_sink.send(lrs).await
    }
}

impl LeaderReplicaState<FileReplica> {
    /// create new replica state using file replica
    pub async fn create_file_replica(
        leader: Replica,
        config: &ConfigOption,
        commands: Sender<LeaderReplicaControllerCommand>,
    ) -> Result<Self, StorageError> {
        trace!(
            "creating new leader replica state: {:#?} using file replica",
            leader
        );

        let storage = create_replica_storage(leader.leader, &leader.id, &config).await?;
        let replica_ids: HashSet<SpuId> = leader.replicas.into_iter().collect();
        Ok(Self::new(
            leader.id,
            leader.leader,
            storage,
            replica_ids,
            commands,
        ))
    }

    /// get start offset and hw
    pub async fn start_offset_info(&self) -> (Offset, Offset) {
        let reader = self.storage.read().await;
        (reader.get_log_start_offset(), reader.get_hw())
    }
    
    /// read records into partition response
    /// return hw and leo
    pub async fn read_records<P>(
        &self,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
        partition_response: &mut P,
    ) -> (Offset, Offset)
    where
        P: SlicePartitionResponse,
    {
        let read_storage = self.storage.read().await;
        match isolation {
            Isolation::ReadCommitted => {
                read_storage
                    .read_committed_records(offset, max_len, partition_response)
                    .await
            }
            Isolation::ReadUncommitted => {
                read_storage
                    .read_records(offset, None, max_len, partition_response)
                    .await
            }
        }
    }

    /// write new record set and update shared offsets
    pub async fn write_record_set(&self, records: &mut RecordSet) -> Result<(), StorageError> {
        let mut writer = self.storage.write().await;
        let offset_updates = writer.write_recordset(records).await?;
        drop(writer);

        match offset_updates {
            OffsetUpdate::Leo(offset) => self.leo.update(offset),
            OffsetUpdate::LeoHw(offset) => self.hw.update(offset),
        }

        Ok(())
    }

    /// sync specific follower
    pub async fn sync_follower(
        &self,
        sinks: &SinkPool<SpuId>,
        follower_id: SpuId,
        follower_info: &FollowerReplicaInfo,
        max_bytes: u32,
    ) {
        if let Some(mut sink) = sinks.get_sink(&follower_id) {
            trace!(
                "sink is found for follower: {}, ready to build sync records",
                follower_id
            );
            let mut sync_request = FileSyncRequest::default();
            let mut topic_response = PeerFileTopicResponse {
                name: self.replica_id.topic.to_owned(),
                ..Default::default()
            };
            let mut partition_response = PeerFilePartitionResponse {
                partition_index: self.replica_id.partition,
                ..Default::default()
            };
            self.read_records(
                follower_info.leo,
                max_bytes,
                Isolation::ReadUncommitted,
                &mut partition_response,
            )
            .await;
            partition_response.last_stable_offset = self.leo();
            partition_response.high_watermark = self.hw();
            topic_response.partitions.push(partition_response);
            sync_request.topics.push(topic_response);

            let request = RequestMessage::new_request(sync_request).set_client_id(format!(
                "leader: {}, replica: {}",
                self.leader_id, self.replica_id
            ));
            debug!(
                "sending records follower: {}, response: {}",
                follower_id, request
            );
            if let Err(err) = sink
                .encode_file_slices(&request, request.header.api_version())
                .await
            {
                error!("error sending file slice: {:#?}", err);
            }
        } else {
            warn!("no sink exits for follower: {}, skipping ", follower_id);
        }
    }

    /// delete storage
    pub async fn remove(&self) -> Result<(), StorageError> {
        let write = self.storage.write().await;
        write.remove().await
    }

    /// synchronize
    pub async fn sync_followers(&self, sinks: &SinkPool<SpuId>, max_bytes: u32) {
        let follower_sync = self.need_follower_updates().await;

        for (follower_id, follower_info) in follower_sync {
            self.sync_follower(sinks, follower_id, &follower_info, max_bytes)
                .await;
        }
    }

    #[allow(dead_code)]
    pub async fn live_replicas(&self) -> Vec<SpuId> {
        self.followers.read().await.keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub fn leader_id(&self) -> SpuId {
        self.leader_id
    }
}

use super::FollowerOffsetUpdate;

#[derive(Debug, Clone, PartialEq)]
pub struct FollowerReplicaInfo {
    hw: Offset,
    leo: Offset,
}

impl Default for FollowerReplicaInfo {
    fn default() -> Self {
        Self { hw: -1, leo: -1 }
    }
}

impl FollowerReplicaInfo {
    /// convert follower ids into BtreeMap of this
    fn ids_to_map(leader_id: SpuId, follower_ids: HashSet<SpuId>) -> BTreeMap<SpuId, Self> {
        let mut followers = BTreeMap::new();
        for id in follower_ids.into_iter().filter(|id| *id != leader_id) {
            followers.insert(id, Self::default());
        }
        followers
    }

    pub fn new(leo: Offset, hw: Offset) -> Self {
        assert!(leo >= hw, "end offset >= high watermark");
        Self { leo, hw }
    }

    pub fn hw(&self) -> Offset {
        self.hw
    }

    pub fn leo(&self) -> Offset {
        self.leo
    }

    pub fn is_same(&self, hw: Offset, leo: Offset) -> bool {
        self.hw == hw && self.leo == leo
    }

    // is valid as long as it's offset are not at default
    pub fn is_valid(&self) -> bool {
        self.hw != -1 && self.leo != -1
    }
}

impl From<(Offset, Offset)> for FollowerReplicaInfo {
    fn from(value: (Offset, Offset)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<S> LeaderReplicaState<S> where S: ReplicaStorage {}

impl LeaderReplicaState<FileReplica> {}

#[cfg(test)]
mod test {

    use std::collections::HashSet;
    use std::iter::FromIterator;

    use async_channel::bounded;
    use fluvio_future::test_async;
    use fluvio_storage::ReplicaStorage;
    use dataplane::Offset;

    use super::LeaderReplicaState;

    struct MockReplica {
        hw: Offset,
        leo: Offset,
    }

    impl MockReplica {
        fn new(leo: Offset, hw: Offset) -> Self {
            MockReplica { hw, leo }
        }
    }

    impl ReplicaStorage for MockReplica {
        fn get_hw(&self) -> Offset {
            self.hw
        }

        fn get_leo(&self) -> Offset {
            self.leo
        }
    }

    #[test_async]
    async fn test_follower_update() -> Result<(), ()> {
        let mock_replica = MockReplica::new(20, 10); // eof, hw
        let (sender, _) = bounded(10);
        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let replica_state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            mock_replica,
            HashSet::from_iter(vec![5001]),
            sender,
        );
        let followers = replica_state.followers.read().await;
        let follower_info = followers.get(&5001).expect("follower should exists");
        assert_eq!(follower_info.hw, -1);
        assert_eq!(follower_info.leo, -1);

        // follower sends update with it's current state 10,10
        // this should trigger status update and follower sync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)).await,
            (true, Some((10, 10).into()))
        );

        // follower resync which sends same offset status, in this case no update but should trigger resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)).await,
            (false, Some((10, 10).into()))
        );

        // finally follower updates the offset, this should trigger update but no resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 10)).await,
            (true, None)
        );

        // follower resync with same value, in this case no update and no resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 10)).await,
            (false, None)
        );
        Ok(())
    }

    #[test_async]
    async fn test_leader_update() -> Result<(), ()> {
        let mock_replica = MockReplica::new(20, 10); // eof, hw

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let (sender, _) = bounded(10);
        let replica_state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            mock_replica,
            HashSet::from_iter(vec![5001]),
            sender,
        );
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        // update high watermark of our replica to same as endoffset
        let mut storage = replica_state.storage.write().await;
        storage.hw = 20;
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)).await,
            (true, Some((10, 10).into()))
        );
        let updates = replica_state.need_follower_updates().await;
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], (5001, (10, 10).into()));

        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 20)).await,
            (true, None)
        );
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        Ok(())
    }
}
