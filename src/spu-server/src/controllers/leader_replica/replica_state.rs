use std::collections::BTreeMap;

use log::debug;
use log::trace;
use log::error;
use log::warn;

use kf_socket::SinkPool;
use kf_protocol::api::RecordSet;
use kf_protocol::api::Offset;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::Isolation;
use flv_metadata::partition::ReplicaKey;
use flv_metadata::partition::Replica;
use internal_api::UpdateLrsRequest;
use flv_storage::FileReplica;
use flv_storage::ConfigOption;
use flv_storage::StorageError;
use flv_types::SpuId;
use flv_types::log_on_err;
use flv_storage::SlicePartitionResponse;
use flv_storage::ReplicaStorage;
use kf_socket::ExclusiveKfSink;

use crate::core::storage::create_replica_storage;
use crate::controllers::follower_replica::FileSyncRequest;
use crate::controllers::follower_replica::PeerFileTopicResponse;
use crate::controllers::follower_replica::PeerFilePartitionResponse;

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

/// Maintain state for Leader replica
#[derive(Debug)]
pub struct LeaderReplicaState<S> {
    replica_id: ReplicaKey,
    leader_id: SpuId,
    followers: BTreeMap<SpuId, FollowerReplicaInfo>,
    storage: S,
}

impl<S> LeaderReplicaState<S> {
    /// create new, followers_id contains leader id
    pub fn new<R>(replica_id: R, leader_id: SpuId, storage: S, follower_ids: Vec<SpuId>) -> Self
    where
        R: Into<ReplicaKey>,
    {
        let mut state = Self {
            replica_id: replica_id.into(),
            leader_id,
            followers: BTreeMap::new(),
            storage,
        };
        state.add_follower_replica(follower_ids);
        state
    }

    pub fn replica_id(&self) -> &ReplicaKey {
        &self.replica_id
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    #[allow(dead_code)]
    pub fn mut_storage(&mut self) -> &mut S {
        &mut self.storage
    }

    /// probably only used in the test
    #[allow(dead_code)]
    pub(crate) fn followers(&self, spu: &SpuId) -> Option<FollowerReplicaInfo> {
        self.followers.get(spu).map(|val| val.clone())
    }

    /// if replica id's doesn't exists, then add, otherwise ignore it
    fn add_follower_replica(&mut self, follower_ids: Vec<SpuId>) {
        let leader_id = self.leader_id;
        for id in follower_ids.into_iter().filter(|id| *id != leader_id) {
            if self.followers.contains_key(&id) {
                warn!(
                    "try to add existing follower id : {} to replica: {}, ignoring",
                    id, self.replica_id
                );
            } else {
                trace!(
                    "inserting: new follower idx for leader: {}, replica: {}, follower: {}",
                    leader_id,
                    self.replica_id,
                    id
                );
                self.followers.insert(id, FollowerReplicaInfo::default());
            }
        }
    }
}

impl<S> LeaderReplicaState<S>
where
    S: ReplicaStorage,
{
    pub fn leo(&self) -> Offset {
        self.storage.get_leo()
    }

    pub fn hw(&self) -> Offset {
        self.storage.get_hw()
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
    pub fn update_follower_offsets<F>(&mut self, offset: F) -> (bool, Option<FollowerReplicaInfo>)
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

        let changed =
            if let Some(old_info) = self.followers.insert(follower_id, follower_info.clone()) {
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
    fn need_follower_updates(&self) -> Vec<(SpuId, FollowerReplicaInfo)> {
        let leo = self.leo();
        let hw = self.hw();

        trace!("computing follower offset for leader: {}, replica: {}, end offset: {}, high watermarkK {}",self.leader_id,self.replica_id,leo,hw);

        self.followers
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
    fn as_lrs_request(&self) -> UpdateLrsRequest {
        let leader = (
            self.leader_id,
            self.storage.get_hw(),
            self.storage.get_leo(),
        )
            .into();
        let replicas = self
            .followers
            .iter()
            .map(|(follower_id, follower_info)| {
                (*follower_id, follower_info.hw(), follower_info.leo()).into()
            })
            .collect();

        UpdateLrsRequest::new(self.replica_id.clone(), leader, replicas)
    }

    pub async fn send_status_to_sc(&self, sc_sink: &ExclusiveKfSink) {
        let mut message = RequestMessage::new_request(self.as_lrs_request());
        message.get_mut_header().set_client_id(format!(
            "spu: {}, replica: {}",
            self.leader_id, self.replica_id
        ));

        log_on_err!(sc_sink.send_request(&message).await);
        debug!(
            "sent replica status to sc: {}, replica: {}",
            self.leader_id, self.replica_id
        );
    }
}

impl LeaderReplicaState<FileReplica> {
    /// create new replica state using file replica
    pub async fn create_file_replica(
        leader: Replica,
        config: &ConfigOption,
    ) -> Result<Self, StorageError> {
        trace!(
            "creating new leader replica state: {:#?} using file replica",
            leader
        );

        let storage = create_replica_storage(leader.leader, &leader.id, &config).await?;

        Ok(Self::new(
            leader.id,
            leader.leader,
            storage,
            leader.replicas,
        ))
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
            let mut topic_response = PeerFileTopicResponse::default();
            topic_response.name = self.replica_id.topic.to_owned();
            let mut partition_response = PeerFilePartitionResponse::default();
            partition_response.partition_index = self.replica_id.partition;
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

    /// synchronize
    pub async fn sync_followers(&self, sinks: &SinkPool<SpuId>, max_bytes: u32) {
        let follower_sync = self.need_follower_updates();

        for (follower_id, follower_info) in follower_sync {
            self.sync_follower(sinks, follower_id, &follower_info, max_bytes)
                .await;
        }
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
        match isolation {
            Isolation::ReadCommitted => {
                self.storage
                    .read_committed_records(offset, max_len, partition_response)
                    .await;
            }
            Isolation::ReadUncommitted => {
                self.storage
                    .read_records(offset, None, max_len, partition_response)
                    .await;
            }
        }

        (self.hw(), self.leo())
    }

    pub async fn send_records(
        &mut self,
        records: RecordSet,
        update_highwatermark: bool,
    ) -> Result<(), StorageError> {
        trace!(
            "writing records to leader: {} replica: {}, ",
            self.leader_id,
            self.replica_id
        );
        self.storage
            .send_records(records, update_highwatermark)
            .await
    }

    #[allow(dead_code)]
    pub fn live_replicas(&self) -> Vec<SpuId> {
        self.followers.keys().cloned().collect()
    }
    #[allow(dead_code)]
    pub fn leader_id(&self) -> SpuId {
        self.leader_id
    }
}

#[cfg(test)]
mod test {

    use flv_storage::ReplicaStorage;
    use kf_protocol::api::Offset;

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

    #[test]
    fn test_follower_update() {
        flv_util::init_logger();
        let mock_replica = MockReplica::new(20, 10); // eof, hw

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let mut replica_state =
            LeaderReplicaState::new(("test", 1), 5000, mock_replica, vec![5001]);
        let follower_info = replica_state
            .followers
            .get(&5001)
            .expect("follower should exists");
        assert_eq!(follower_info.hw, -1);
        assert_eq!(follower_info.leo, -1);

        // follower sends update with it's current state 10,10
        // this should trigger status update and follower sync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)),
            (true, Some((10, 10).into()))
        );

        // follower resync which sends same offset status, in this case no update but should trigger resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)),
            (false, Some((10, 10).into()))
        );

        // finally follower updates the offset, this should trigger update but no resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 10)),
            (true, None)
        );

        // follower resync with same value, in this case no update and no resync
        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 10)),
            (false, None)
        );
    }

    #[test]
    fn test_leader_update() {
        flv_util::init_logger();
        let mock_replica = MockReplica::new(20, 10); // eof, hw

        // inserting new replica state, this should set follower offset to -1,-1 as inital state
        let mut replica_state =
            LeaderReplicaState::new(("test", 1), 5000, mock_replica, vec![5001]);
        assert_eq!(replica_state.need_follower_updates().len(), 0);

        // update high watermark of our replica to same as endoffset
        replica_state.mut_storage().hw = 20;
        assert_eq!(replica_state.need_follower_updates().len(), 0);

        assert_eq!(
            replica_state.update_follower_offsets((5001, 10, 10)),
            (true, Some((10, 10).into()))
        );
        let updates = replica_state.need_follower_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], (5001, (10, 10).into()));

        assert_eq!(
            replica_state.update_follower_offsets((5001, 20, 20)),
            (true, None)
        );
        assert_eq!(replica_state.need_follower_updates().len(), 0);
    }
}
