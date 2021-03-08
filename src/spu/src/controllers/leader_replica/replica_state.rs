use std::{
    cmp::min,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use std::fmt;

use tracing::instrument;
use tracing::{debug, trace, error, warn};
use async_rwlock::{RwLock};
use async_channel::{Sender, SendError};

use fluvio_socket::SinkPool;
use dataplane::record::RecordSet;
use dataplane::{Offset, Isolation};
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::partition::{ReplicaKey, Replica};
use fluvio_controlplane::LrsRequest;
use fluvio_storage::{FileReplica, StorageError, SlicePartitionResponse, ReplicaStorage};
use fluvio_types::{SpuId, event::offsets::OffsetChangeListener};
use fluvio_types::event::offsets::OffsetPublisher;

use crate::{config::SpuConfig, controllers::sc::SharedSinkMessageChannel};
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
    spu_config: Arc<SpuConfig>,
    leader_id: SpuId,
    storage: RwLock<S>,
    followers: RwLock<BTreeMap<SpuId, FollowerReplicaInfo>>,
    leo: Arc<OffsetPublisher>,
    hw: Arc<OffsetPublisher>,
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
        spu_config: Arc<SpuConfig>,
        storage: S,
        follower_ids: HashSet<SpuId>,
        commands: Sender<LeaderReplicaControllerCommand>,
    ) -> Self
    where
        R: Into<ReplicaKey>,
    {
        let leo = Arc::new(OffsetPublisher::new(storage.get_leo()));
        let hw = Arc::new(OffsetPublisher::new(storage.get_hw()));
        let followers = FollowerReplicaInfo::ids_to_map(leader_id, follower_ids);
        Self {
            replica_id: replica_id.into(),
            leader_id,
            spu_config,
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

    /// listen to offset based on isolation
    pub fn offset_listener(&self, isolation: &Isolation) -> OffsetChangeListener {
        match isolation {
            Isolation::ReadCommitted => self.hw.change_listner(),
            Isolation::ReadUncommitted => self.leo.change_listner(),
        }
    }

    // probably only used in the test
    /*
    #[allow(dead_code)]
    pub(crate) fn followers(&self, spu: &SpuId) -> Option<FollowerReplicaInfo> {
        self.followers.read().await.get(spu).cloned()
    }
    */

    /// send message to leader controller
    pub async fn send_message_to_controller(
        &self,
        command: LeaderReplicaControllerCommand,
    ) -> Result<(), SendError<LeaderReplicaControllerCommand>> {
        self.commands.send(command).await
    }

    /// compute hw based on updates follow
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
    ///
    ///  Simple HW mark calculation (assume LSR = 2) which is find minimum offset values that satisfy
    ///     Assume: Leader leo = 10, hw = 2,
    ///         follower: leo(2,4)  =>   no change, since it doesn't satisfy minim LSR
    ///         follower: leo(3,4)  =>   hw = 3  that is smallest leo that satisfy
    ///         follower: leo(4,4)  =>   hw = 4
    ///         follower: leo(6,7,9) =>  hw = 7,

    pub async fn update_followers<F>(
        &self,
        offset: F,
    ) -> (bool, Option<FollowerReplicaInfo>, Option<Offset>)
    where
        F: Into<FollowerOffsetUpdate>,
    {
        let follower_offset = offset.into();

        let follower_id = follower_offset.follower_id;
        let mut follower_info = FollowerReplicaInfo::new(follower_offset.leo, follower_offset.hw);

        let leader_leo = self.leo();
        let leader_hw = self.hw();

        // if update offset is greater than leader than something is wrong, in this case
        // we truncate the the follower offset

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

        let resync_flag = if leader_leo != follower_info.leo || leader_hw != follower_info.hw {
            Some(follower_info)
        } else {
            None
        };

        // if our leo and hw is same there is no need to recompute hw
        if leader_leo == leader_hw {
            (changed, resync_flag, None)
        } else {
            let min_lsr = min(
                self.spu_config.replication.min_in_sync_replicas,
                followers.len() as u16,
            );
            // compute unique offsets that is greater than min leader's HW
            let qualified_leos: Vec<Offset> = followers
                .values()
                .filter_map(|follower_info| {
                    if follower_info.leo > leader_hw {
                        Some(follower_info.leo)
                    } else {
                        None
                    }
                })
                .collect();

            //debug!("qualified: {:#?}", qualified_leos);

            let mut unique_leos = qualified_leos.clone();
            unique_leos.dedup();

            // debug!("unique_leos: {:#?}", unique_leos);

            let mut hw_list: Vec<Offset> = unique_leos
                .iter()
                .filter_map(|unique_offset| {
                    // leo must have at least must have replicated min_lsr
                    if (qualified_leos
                        .iter()
                        .filter(|leo| unique_offset <= leo)
                        .count() as u16)
                        >= min_lsr
                    {
                        Some(*unique_offset)
                    } else {
                        None
                    }
                })
                .collect();

            hw_list.sort_unstable();

            (changed, resync_flag, hw_list.pop())
        }
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

    /// write new record set and update shared offsets
    #[instrument(skip(self))]
    pub async fn write_record_set(&self, records: &mut RecordSet) -> Result<(), StorageError> {
        let hw_update = self.spu_config.replication.min_in_sync_replicas == 1;
        let mut writer = self.storage.write().await;
        let _offset_updates = writer.write_recordset(records, hw_update).await?;

        let leo = writer.get_leo();
        debug!(leo);
        self.leo.update(writer.get_leo());
        if hw_update {
            let hw = writer.get_hw();
            debug!(hw);
            self.hw.update(hw);
        }

        Ok(())
    }

    pub async fn update_hw(&self, hw: Offset) -> Result<bool, StorageError> {
        let mut writer = self.storage.write().await;
        writer.update_high_watermark(hw).await
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
        P: SlicePartitionResponse + Send,
    {
        let read_storage = self.storage.read().await;

        read_storage
            .read_partition_slice(offset, max_len, isolation, partition_response)
            .await
    }
}

impl LeaderReplicaState<FileReplica> {
    /// create new replica state using file replica
    pub async fn create_file_replica(
        leader: Replica,
        config: Arc<SpuConfig>,
        commands: Sender<LeaderReplicaControllerCommand>,
    ) -> Result<Self, StorageError> {
        trace!(
            "creating new leader replica state: {:#?} using file replica",
            leader
        );

        let storage_config = config.storage().new_config();
        let storage = create_replica_storage(leader.leader, &leader.id, &storage_config).await?;
        let replica_ids: HashSet<SpuId> = leader.replicas.into_iter().collect();
        Ok(Self::new(
            leader.id,
            leader.leader,
            config,
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

/// convert tuple of (leo,hw)
impl From<(Offset, Offset)> for FollowerReplicaInfo {
    fn from(value: (Offset, Offset)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<S> LeaderReplicaState<S> where S: ReplicaStorage {}

impl LeaderReplicaState<FileReplica> {}

#[cfg(test)]
mod test {

    use std::{collections::HashSet, sync::Arc};
    use std::iter::FromIterator;

    use async_channel::bounded;
    use async_trait::async_trait;

    use fluvio_future::test_async;
    use fluvio_storage::{ReplicaStorage};
    use dataplane::Offset;
    use dataplane::record::RecordSet;

    use crate::config::SpuConfig;
    use super::LeaderReplicaState;

    struct MockReplica {
        hw: Offset,
        leo: Offset,
        hw_update: Option<Offset>,
    }

    impl MockReplica {
        fn new(leo: Offset, hw: Offset) -> Self {
            MockReplica {
                hw,
                leo,
                hw_update: None,
            }
        }
    }

    #[async_trait]
    impl ReplicaStorage for MockReplica {
        fn get_hw(&self) -> Offset {
            self.hw
        }

        fn get_leo(&self) -> Offset {
            self.leo
        }

        async fn read_partition_slice<P>(
            &self,
            _offset: Offset,
            _max_len: u32,
            _isolation: dataplane::Isolation,
            _partition_response: &mut P,
        ) -> (Offset, Offset)
        where
            P: fluvio_storage::SlicePartitionResponse + Send,
        {
            todo!()
        }

        // do dummy implementations of write
        async fn write_recordset(
            &mut self,
            _records: &mut dataplane::record::RecordSet,
            _update_highwatermark: bool,
        ) -> Result<(), fluvio_storage::StorageError> {
            let _ = self.hw_update.take();
            Ok(())
        }

        async fn update_high_watermark(
            &mut self,
            _offset: Offset,
        ) -> Result<bool, fluvio_storage::StorageError> {
            todo!()
        }
    }

    // test hw calculation for 2 spu and 2 in sync replicas
    #[test_async]
    async fn test_follower_hw22() -> Result<(), ()> {
        let mut spu_config = SpuConfig::default();
        spu_config.replication.min_in_sync_replicas = 2;
        let config = Arc::new(spu_config);
        let mock_replica = MockReplica::new(10, 2); // leo, hw
        let (sender, _) = bounded(10);

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            config,
            mock_replica,
            HashSet::from_iter(vec![5001, 5002]),
            sender,
        );

        // ensure all followers initialized to -1
        let followers = state.followers.read().await;
        let follower1 = followers.get(&5001).expect("5001");
        assert_eq!(follower1.hw, -1);
        assert_eq!(follower1.leo, -1);
        let follower2 = followers.get(&5002).expect("5002");
        assert_eq!(follower2.hw, -1);
        assert_eq!(follower2.leo, -1);
        drop(followers);

        assert_eq!(state.leo(), 10);
        assert_eq!(state.hw(), 2);

        // follower sends leo=4,hw = 2
        // status = true, Some(4,2), None
        assert_eq!(
            state.update_followers((5001, 4, 2)).await,
            (true, Some((4, 2).into()), None)
        );

        let followers = state.followers.read().await;
        let follower_info = followers.get(&5001).expect("5001");
        assert_eq!(follower_info.hw, 2);
        assert_eq!(follower_info.leo, 4);
        drop(followers);

        // 2nd spu send leo = 2, hw = 2,
        // status = true, Some(2,2), None
        assert_eq!(
            state.update_followers((5002, 2, 2)).await,
            (true, Some((2, 2).into()), None)
        );

        // 1nd spu send different leo, 1 spu need to resync since it's not fully sync with leader
        // status = false, Some(3,2), None
        assert_eq!(
            state.update_followers((5001, 3, 2)).await,
            (true, Some((3, 2).into()), None)
        );

        // 2nd spu send leo = 4, hw = 2,  it has caught more than 1 spu but since 2nd sp
        // is still behind new hw = 3
        // status = true, Some(2,2), None
        assert_eq!(
            state.update_followers((5002, 4, 2)).await,
            (true, Some((4, 2).into()), Some(3))
        );

        // 1nd spu send leo = 4, hw = 2,
        // both followers have caught up
        // status = true, Some(2,2), None
        assert_eq!(
            state.update_followers((5001, 4, 2)).await,
            (true, Some((4, 2).into()), Some(4))
        );

        Ok(())
    }

    // test hw calculation for 3 spu and 2 in sync rep
    #[test_async]
    async fn test_follower_hw32() -> Result<(), ()> {
        let mut spu_config = SpuConfig::default();
        spu_config.replication.min_in_sync_replicas = 2;
        let config = Arc::new(spu_config);
        let mock_replica = MockReplica::new(10, 2); // leo, hw
        let (sender, _) = bounded(10);

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            config,
            mock_replica,
            HashSet::from_iter(vec![5001, 5002, 5003]),
            sender,
        );

        // leo(6,7,9) => 7
        state.update_followers((5001, 6, 2)).await;
        state.update_followers((5002, 7, 2)).await;
        assert_eq!(
            state.update_followers((5003, 9, 2)).await,
            (true, Some((9, 2).into()), Some(7))
        );

        // leo(9,7,9) => 9
        assert_eq!(
            state.update_followers((5001, 9, 2)).await,
            (true, Some((9, 2).into()), Some(9))
        );

        Ok(())
    }

    // test hw calculation for 3 spu and 3 in sync rep
    #[test_async]
    async fn test_follower_hw33() -> Result<(), ()> {
        let mut spu_config = SpuConfig::default();
        spu_config.replication.min_in_sync_replicas = 3;
        let config = Arc::new(spu_config);
        let mock_replica = MockReplica::new(10, 2); // leo, hw
        let (sender, _) = bounded(10);

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            config,
            mock_replica,
            HashSet::from_iter(vec![5001, 5002, 5003]),
            sender,
        );

        // only 2 is satisifed so no HW
        state.update_followers((5001, 6, 2)).await;
        assert_eq!(
            state.update_followers((5002, 7, 2)).await,
            (true, Some((7, 2).into()), None)
        );

        assert_eq!(
            state.update_followers((5003, 9, 2)).await,
            (true, Some((9, 2).into()), Some(6))
        );

        // leo(9,7,9) => 9
        assert_eq!(
            state.update_followers((5001, 9, 2)).await,
            (true, Some((9, 2).into()), Some(7))
        );

        Ok(())
    }

    #[test_async]
    async fn test_leader_update() -> Result<(), ()> {
        let mock_replica = MockReplica::new(20, 10); // leo, hw
        let spu_config = Arc::new(SpuConfig::default());

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let (sender, _) = bounded(10);
        let replica_state = LeaderReplicaState::new(
            ("test", 1),
            5000,
            spu_config,
            mock_replica,
            HashSet::from_iter(vec![5001]),
            sender,
        );
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        // update high watermark of our replica to same as endoffset
        let mut storage = replica_state.storage.write().await;
        storage.hw_update = Some(20);
        drop(storage);

        replica_state
            .write_record_set(&mut RecordSet::default())
            .await
            .expect("write");

        // since we don't have followers, no updates need
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        // add follower offsets info
        assert_eq!(
            replica_state.update_followers((5001, 10, 10)).await,
            (true, Some((10, 10).into()), None)
        );
        let updates = replica_state.need_follower_updates().await;
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], (5001, (10, 10).into()));

        assert_eq!(
            replica_state.update_followers((5001, 20, 10)).await,
            (true, None, Some(20))
        );
        assert_eq!(replica_state.need_follower_updates().await.len(), 0);

        Ok(())
    }
}
