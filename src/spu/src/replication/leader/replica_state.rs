use std::{
    cmp::min,
    collections::{BTreeMap, HashSet},
    ops::{Deref, DerefMut},
    sync::Arc,
};
use std::iter::FromIterator;
use std::fmt;

use tracing::{debug, trace, error, warn};
use tracing::instrument;
use async_rwlock::{RwLock};
use async_channel::{Sender, Receiver, SendError};

use fluvio_socket::SinkPool;
use dataplane::{ReplicaKey, record::RecordSet};
use dataplane::{Offset, Isolation};
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::partition::{Replica};
use fluvio_controlplane::LrsRequest;
use fluvio_storage::{FileReplica, StorageError, ReplicaStorage, OffsetInfo};
use fluvio_types::{SpuId};

use crate::{
    config::{ReplicationConfig},
    control_plane::SharedSinkMessageChannel,
};
use crate::replication::follower::sync::{
    FileSyncRequest, PeerFileTopicResponse, PeerFilePartitionResponse,
};
use super::super::follower::FollowerReplicaState;
use crate::storage::SharableReplicaStorage;

pub type SharedLeaderState<S> = LeaderReplicaState<S>;
pub type SharedFileLeaderState = LeaderReplicaState<FileReplica>;

use super::LeaderReplicaControllerCommand;

#[derive(Debug)]
pub struct LeaderReplicaState<S> {
    leader: SpuId,
    storage: SharableReplicaStorage<S>,
    config: ReplicationConfig,
    followers: Arc<RwLock<BTreeMap<SpuId, OffsetInfo>>>,
    sender: Sender<LeaderReplicaControllerCommand>,
}

impl<S> Clone for LeaderReplicaState<S> {
    fn clone(&self) -> Self {
        Self {
            leader: self.leader.clone(),
            storage: self.storage.clone(),
            config: self.config.clone(),
            followers: self.followers.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<S> fmt::Display for LeaderReplicaState<S>
where
    S: ReplicaStorage,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Leader for {}", self.id())
    }
}

impl<S> Deref for LeaderReplicaState<S> {
    type Target = SharableReplicaStorage<S>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<S> DerefMut for LeaderReplicaState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

/// convert follower ids into BtreeMap of this
fn ids_to_map(leader_id: SpuId, follower_ids: HashSet<SpuId>) -> BTreeMap<SpuId, OffsetInfo> {
    let mut followers = BTreeMap::new();
    for id in follower_ids.into_iter().filter(|id| *id != leader_id) {
        followers.insert(id, OffsetInfo::default());
    }
    followers
}

impl<S> LeaderReplicaState<S>
where
    S: ReplicaStorage,
{
    /// create ne state from existing storage
    pub fn new(
        replica: Replica,
        config: ReplicationConfig,
        inner: SharableReplicaStorage<S>,
        sender: Sender<LeaderReplicaControllerCommand>,
    ) -> Self {
        let follower_ids = HashSet::from_iter(replica.replicas);
        let followers = ids_to_map(replica.leader, follower_ids);
        Self {
            leader: replica.leader,
            storage: inner,
            config,
            followers: Arc::new(RwLock::new(followers)),
            sender,
        }
    }

    /// create new complete state and spawn controller
    pub async fn create<'a, C>(
        replica: Replica,
        config: &'a C,
    ) -> Result<
        (
            LeaderReplicaState<S>,
            Receiver<LeaderReplicaControllerCommand>,
        ),
        StorageError,
    >
    where
        ReplicationConfig: From<&'a C>,
        S::Config: From<&'a C>,
    {
        use async_channel::bounded;

        let (sender, receiver) = bounded(10);

        let inner = SharableReplicaStorage::create(replica.id.clone(), config.into()).await?;

        let leader_replica = Self::new(replica, config.into(), inner, sender);
        Ok((leader_replica, receiver))
    }

    pub fn promoted_from(
        follower: FollowerReplicaState<S>,
        replica: Replica,
        config: ReplicationConfig,
        sender: Sender<LeaderReplicaControllerCommand>,
    ) -> Self {
        let replica_storage = follower.inner_owned();
        Self::new(replica, config, replica_storage, sender)
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
        self.sender.send(command).await
    }

    pub fn replica(&self) -> &ReplicaKey {
        self.storage.id()
    }

    pub fn as_offset(&self) -> OffsetInfo {
        OffsetInfo {
            hw: self.hw(),
            leo: self.leo(),
        }
    }

    /// update hw based on offset change
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
    #[instrument(skip(self))]
    pub async fn update_hw_from_followers(&self, follower_id: SpuId, follower_pos: OffsetInfo) {
        let leader_pos = self.as_offset();

        // follower must be always behind leader

        if follower_pos.newer(&leader_pos) {
            error!("follower pos must not be newer",);
            return;
        }

        // get follower info
        let mut followers = self.followers.write().await;
        if let Some(current_follow_info) = followers.get_mut(&follower_id) {
            if current_follow_info.update(&follower_pos) {
                // if our leo and hw is same there is no need to recompute hw
                if !leader_pos.is_committed() {
                    if let Some(hw) =
                        compute_hw(&leader_pos, self.config.min_in_sync_replicas, &followers)
                    {
                        debug!(hw, "updating hw");
                        if let Err(err) = self.update_hw(hw).await {
                            error!("error updating hw: {}", err);
                        };
                    } else {
                        debug!("no change");
                    }
                } else {
                    debug!("leader is committed");
                }
            }
        } else {
            error!(follower_id, "invalid follower");
        }
    }

    /// compute list of followers that need to be sync
    /// this is done by checking diff of end offset and high watermark
    async fn need_follower_updates(&self) -> Vec<(SpuId, OffsetInfo)> {
        let offset = self.as_offset();
        trace!(
            "computing follower offset for leader: {}, {:#?}",
            self.id(),
            offset
        );

        let reader = self.followers.read().await;
        reader
            .iter()
            .filter(|(_, follower_info)| {
                follower_info.is_valid() && !follower_info.is_same(&offset)
            })
            .map(|(follower_id, follower_info)| {
                debug!(
                    replica = %self.id(),
                    follower_id,
                    "needs to be updated"
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
        let leader = (self.leader.to_owned(), self.hw(), self.leo()).into();
        let replicas = self
            .followers
            .read()
            .await
            .iter()
            .map(|(follower_id, follower_info)| {
                (*follower_id, follower_info.hw, follower_info.leo).into()
            })
            .collect();

        LrsRequest::new(self.id().to_owned(), leader, replicas)
    }

    #[instrument(skip(self, sc_sink))]
    pub async fn send_status_to_sc(&self, sc_sink: &SharedSinkMessageChannel) {
        let lrs = self.as_lrs_request().await;
        debug!(hw = lrs.leader.hw, leo = lrs.leader.leo);
        sc_sink.send(lrs).await
    }

    pub async fn write_record_set(&self, records: &mut RecordSet) -> Result<(), StorageError> {
        self.storage
            .write_record_set(records, self.config.min_in_sync_replicas == 1)
            .await
    }

    #[allow(dead_code)]
    pub async fn live_replicas(&self) -> Vec<SpuId> {
        self.followers.read().await.keys().cloned().collect()
    }

    /// synchronize
    pub async fn sync_followers(&self, sinks: &SinkPool<SpuId>, max_bytes: u32) {
        let follower_sync = self.need_follower_updates().await;

        for (follower_id, follower_info) in follower_sync {
            self.sync_follower(sinks, follower_id, &follower_info, max_bytes)
                .await;
        }
    }

    /// sync specific follower
    #[instrument(skip(self, sinks, follower_info))]
    pub async fn sync_follower(
        &self,
        sinks: &SinkPool<SpuId>,
        follower_id: SpuId,
        follower_info: &OffsetInfo,
        max_bytes: u32,
    ) {
        if let Some(mut sink) = sinks.get_sink(&follower_id) {
            trace!("ready to build sync records");
            let mut sync_request = FileSyncRequest::default();
            let mut topic_response = PeerFileTopicResponse {
                name: self.id().topic.to_owned(),
                ..Default::default()
            };
            let mut partition_response = PeerFilePartitionResponse {
                partition: self.id().partition,
                ..Default::default()
            };
            let offset = self
                .read_records(
                    follower_info.leo,
                    max_bytes,
                    Isolation::ReadUncommitted,
                    &mut partition_response,
                )
                .await;
            debug!(
                follower_id,
                hw = offset.hw,
                leo = offset.leo,
                len = partition_response.records.len(),
                "sending records"
            );
            // ensure leo and hw are set correctly. storage might have update last stable offset
            partition_response.leo = offset.leo;
            partition_response.hw = offset.hw;
            topic_response.partitions.push(partition_response);
            sync_request.topics.push(topic_response);

            let request = RequestMessage::new_request(sync_request).set_client_id(format!(
                "leader: {}, replica: {}",
                self.leader,
                self.id()
            ));

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
}

/// compute leader's updated hw based on follower offset
/// this is done after follower's leo updated
/// min_replica must be at least 1 and must be less than followers.len(0)
fn compute_hw(
    leader: &OffsetInfo,
    min_replica: u16,
    followers: &BTreeMap<SpuId, OffsetInfo>,
) -> Option<Offset> {
    assert!(min_replica > 0);
    assert!((min_replica - 1) <= followers.len() as u16);
    let min_lsr = min(min_replica - 1, followers.len() as u16);
    //println!("min lsr: {}", min_lsr);

    // compute unique offsets that is greater than min leader's HW
    let qualified_leos: Vec<Offset> = followers
        .values()
        .filter_map(|follower_info| {
            let leo = follower_info.leo;
            if leo > leader.hw {
                Some(leo)
            } else {
                None
            }
        })
        .collect();

    if qualified_leos.is_empty() {
        return None;
    }

    //println!("qualified: {:#?}", qualified_leos);

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
    hw_list.pop()
}

impl<S> LeaderReplicaState<S> where S: ReplicaStorage {}

impl LeaderReplicaState<FileReplica> {}

/*
#[cfg(test)]
mod mock_replica {

    use async_trait::async_trait;

    //use fluvio_future::test_async;
    use fluvio_controlplane_metadata::partition::{ReplicaKey};
    //use fluvio_controlplane_metadata::partition::Replica;
    use fluvio_storage::{OffsetInfo};
    use dataplane::Offset;


    use crate::{
        config::{Log},
        storage::SharableReplicaStorage,
    };
    use super::*;

    #[derive(Default)]
    struct MockConfig {
        hw: Offset,
        leo: Offset,
    }

    impl ReplicaStorageConfig for MockConfig {}

    #[derive(Default)]
    struct MockReplica {
        hw: Offset,
        leo: Offset,
        hw_update: Option<Offset>,
    }

    impl MockReplica {
        async fn create(
            leo: Offset,
            hw: Offset,
            id: ReplicaKey,
        ) -> Result<SharableReplicaStorage<Self>, StorageError> {
            let config = MockConfig { hw, leo };
            SharableReplicaStorage::create(id, config).await
        }
    }

    impl From<&Log> for MockConfig {
        fn from(_log: &Log) -> MockConfig {
            MockConfig::default()
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
        ) -> OffsetInfo
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

        type Config = MockConfig;

        async fn create(
            _replica: &dataplane::ReplicaKey,
            config: Self::Config,
        ) -> Result<Self, fluvio_storage::StorageError> {
            Ok(MockReplica {
                hw: config.hw,
                leo: config.leo,
                ..Default::default()
            })
        }

        fn get_log_start_offset(&self) -> Offset {
            todo!()
        }

        async fn remove(&self) -> Result<(), fluvio_storage::StorageError> {
            todo!()
        }
    }

}
*/

#[cfg(test)]
mod test_hw_updates {

    use super::*;

    fn offsets_maps(offsets: Vec<(SpuId, OffsetInfo)>) -> BTreeMap<SpuId, OffsetInfo> {
        offsets.into_iter().collect()
    }

    /// test min lsr check
    #[test]
    #[should_panic]
    fn test_hw_min_lsr_invalid_hw() {
        compute_hw(&OffsetInfo { hw: 0, leo: 10 }, 0, &offsets_maps(vec![]));
    }

    #[test]
    #[should_panic]
    fn test_hw_min_lsr_too_much() {
        compute_hw(
            &OffsetInfo { hw: 0, leo: 10 },
            3,
            &offsets_maps(vec![(5001, OffsetInfo::default())]),
        );
    }

    // test hw calculation for 2 spu and 2 in sync replicas
    #[test]
    fn test_hw22() {
        // starts with leader leo=10,hw = 0

        // initially, we don't have no information about follower,
        // our hw doesn't need to be updated
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 0, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo::default())])
            ),
            None
        );

        // followers send back leo = 4, hw = 0
        // This cause hw = 4 since this is min that is replicated across 2 SPU
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 0, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo { leo: 4, hw: 0 })])
            ),
            Some(4)
        );

        //  we send back follower hw = 4
        //  followers send back leo = 6, hw = 4
        //  This should update hw = 6
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 4, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo { leo: 6, hw: 4 })])
            ),
            Some(6)
        );

        //  follower send back same info, since min LEO didn't update, no hw update
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 6, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo { leo: 6, hw: 6 })])
            ),
            None
        );

        //  follower now fully caught up, leo = 10, hw = 6
        //  hw should be now 10
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 6, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo { leo: 10, hw: 6 })])
            ),
            Some(10)
        );

        // followers send back same, no hw update
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 10, leo: 10 },
                2,
                &offsets_maps(vec![(5001, OffsetInfo { leo: 10, hw: 10 })])
            ),
            None
        );
    }

    // test hw calculation for 3 spu with 2 in replica
    #[test]
    fn test_hw32() {
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 0, leo: 10 },
                2,
                &offsets_maps(vec![
                    (5001, OffsetInfo::default()),
                    (5002, OffsetInfo::default())
                ])
            ),
            None
        );

        // hw updated when at least 1 SPU replicated offsets
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 0 },
                2,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo::default())
                ])
            ),
            Some(4)
        );

        // we take maximum leo since min lsr = 2
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 0 },
                2,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo { leo: 6, hw: 0 })
                ])
            ),
            Some(6)
        );

        // test with 3 followers
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 0 },
                2,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo { leo: 6, hw: 0 }),
                    (5003, OffsetInfo { leo: 9, hw: 0 })
                ])
            ),
            Some(9)
        );

        // none of the follower has catch up
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 7 },
                2,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo { leo: 6, hw: 0 })
                ])
            ),
            None
        );
    }

    // test hw calculation for 3 spu and 3 in sync rep
    #[test]
    fn test_hw33() {
        assert_eq!(
            compute_hw(
                &OffsetInfo { hw: 0, leo: 10 },
                3,
                &offsets_maps(vec![
                    (5001, OffsetInfo::default()),
                    (5002, OffsetInfo::default()),
                ])
            ),
            None
        );

        // need at least 2 replicas
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 0 },
                3,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo::default())
                ])
            ),
            None
        );

        // 4 is min offset
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 0 },
                3,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 4, hw: 0 }),
                    (5002, OffsetInfo { leo: 7, hw: 0 }),
                ])
            ),
            Some(4)
        );

        // no hw update since nothing with 2 followers has replicated
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 7 },
                3,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 7, hw: 6 }),
                    (5002, OffsetInfo { leo: 8, hw: 6 }),
                ])
            ),
            None
        );

        // leader can progress to 8
        assert_eq!(
            compute_hw(
                &OffsetInfo { leo: 10, hw: 7 },
                3,
                &offsets_maps(vec![
                    (5001, OffsetInfo { leo: 9, hw: 6 }),
                    (5002, OffsetInfo { leo: 8, hw: 6 }),
                ])
            ),
            Some(8)
        );
    }
}
