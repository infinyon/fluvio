use std::{
    cmp::{min, Reverse},
    collections::{BTreeMap, HashSet, BinaryHeap},
    ops::{Deref, DerefMut},
    sync::Arc,
};
use std::iter::FromIterator;
use std::fmt;

use tokio::sync::Mutex;
use fluvio_controlplane::{replica::Replica, sc_api::update_lrs::LrsRequest};
use tracing::{debug, error, warn};
use tracing::instrument;
use tokio::sync::RwLock;
use anyhow::{Result, Context};

use fluvio_protocol::record::{RecordSet, Offset, ReplicaKey, RawRecords, Batch};
use fluvio_controlplane_metadata::partition::{PartitionMirrorConfig, PartitionStatus, ReplicaStatus};
use fluvio_storage::{FileReplica, ReplicaStorage, OffsetInfo, ReplicaStorageConfig};
use fluvio_types::{
    event::offsets::{SharedOffsetPublisher, WeakSharedOffsetPublisher, TOPIC_DELETED},
    SpuId,
};
use fluvio_spu_schema::{Isolation, COMMON_VERSION};

use crate::{
    config::ReplicationConfig,
    control_plane::SharedLrsStatusUpdate,
    core::GlobalContext,
    mirroring::remote::controller::{MirrorRemoteToHomeController, SharedMirrorControllerState},
    smartengine::{
        batch::process_record_set,
        context::{SharedSmartModuleContext, SmartModuleContext},
        dedup_to_invocation,
    },
};
use crate::replication::follower::sync::{PeerFileTopicResponse, PeerFilePartitionResponse};
use crate::storage::SharableReplicaStorage;

use super::FollowerNotifier;

pub type SharedLeaderState<S> = LeaderReplicaState<S>;
pub type SharedFileLeaderState = LeaderReplicaState<FileReplica>;

pub const CLEANUP_FREQUENCY: usize = 10;

#[derive(Debug)]
pub struct LeaderReplicaState<S> {
    replica: Replica,
    in_sync_replica: u16,
    storage: SharableReplicaStorage<S>,
    config: ReplicationConfig,
    followers: Arc<RwLock<BTreeMap<SpuId, OffsetInfo>>>,
    status_update: SharedLrsStatusUpdate,
    sm_ctx: Option<SharedSmartModuleContext>,
    consumer_offset_publishers: Arc<Mutex<Vec<WeakSharedOffsetPublisher>>>,
    mirror_controller_state: Option<SharedMirrorControllerState>,
}

impl<S> Clone for LeaderReplicaState<S> {
    fn clone(&self) -> Self {
        Self {
            replica: self.replica.clone(),
            storage: self.storage.clone(),
            config: self.config.clone(),
            followers: self.followers.clone(),
            in_sync_replica: self.in_sync_replica,
            status_update: self.status_update.clone(),
            sm_ctx: self.sm_ctx.clone(),
            consumer_offset_publishers: self.consumer_offset_publishers.clone(),
            mirror_controller_state: self.mirror_controller_state.clone(),
        }
    }
}

impl<S> fmt::Display for LeaderReplicaState<S>
where
    S: ReplicaStorage,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Leader state for {}", self.id())
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
    /// create new state from existing storage
    /// calculate default in_sync_replica from followers
    pub fn new(
        replica: Replica,
        config: ReplicationConfig,
        status_update: SharedLrsStatusUpdate,
        inner: SharableReplicaStorage<S>,
    ) -> Uninit<Self> {
        debug!(?replica, "replica storage");
        let in_sync_replica = replica.replicas.len() as u16;
        let follower_ids = HashSet::from_iter(replica.replicas.clone());
        let followers = ids_to_map(replica.leader, follower_ids);
        debug!(?followers, "leader followers");

        debug!(
            in_sync_replica,
            replica = %replica.id,
            follower = ?replica.replicas,
            "creating leader"
        );

        Uninit(Self {
            replica,
            storage: inner,
            config,
            followers: Arc::new(RwLock::new(followers)),
            in_sync_replica,
            status_update,
            sm_ctx: None,
            consumer_offset_publishers: Arc::new(Mutex::new(Vec::new())),
            mirror_controller_state: None,
        })
    }

    /// create new complete state and spawn controller
    pub async fn create<'a, C>(
        replica: Replica,
        config: &'a C,
        status_update: SharedLrsStatusUpdate,
    ) -> Result<Uninit<LeaderReplicaState<S>>>
    where
        ReplicationConfig: From<&'a C>,
        S::ReplicaConfig: From<&'a C>,
    {
        let mut replica_config: S::ReplicaConfig = config.into();
        replica_config.update_from_replica(&replica);
        let inner = SharableReplicaStorage::create(replica.id.clone(), replica_config).await?;
        let leader_replica = Self::new(replica, config.into(), status_update, inner);
        leader_replica.0.update_status().await;
        Ok(leader_replica)
    }

    /// replica id
    pub fn id(&self) -> &ReplicaKey {
        &self.replica.id
    }

    /// leader SPU. This should be same as our local SPU
    pub fn leader(&self) -> SpuId {
        self.replica.leader
    }

    /// replica metadata
    pub fn get_replica(&self) -> &Replica {
        &self.replica
    }

    /// override in sync replica
    #[allow(unused)]
    fn set_in_sync_replica(&mut self, replica_count: u16) {
        self.in_sync_replica = replica_count;
    }

    /// update leader's state from follower's offset states
    /// if follower's state has been updated may result in leader's hw update
    /// return true if update has been updated, in this case, updates can be computed to followers
    /// return false if no change in state leader
    #[instrument(skip(self, notifier))]
    pub async fn update_states_from_followers(
        &self,
        follower_id: SpuId,
        follower_pos: OffsetInfo,
        notifier: &FollowerNotifier,
    ) -> bool {
        let leader_pos = self.as_offset();

        // follower must be always behind leader

        if follower_pos.newer(&leader_pos) {
            warn!(?follower_pos, ?leader_pos, "follower pos must not be newer");
            return false;
        }

        // get follower info
        let mut followers = self.followers.write().await;
        let update = if let Some(current_follow_info) = followers.get_mut(&follower_id) {
            if current_follow_info.update(&follower_pos) {
                // if our leo and hw is same there is no need to recompute hw
                if !leader_pos.is_committed() {
                    if let Some(hw) = compute_hw(&leader_pos, self.in_sync_replica, &followers) {
                        debug!(hw, "updating hw");
                        if let Err(err) = self.update_hw(hw).await {
                            error!("error updating hw: {}", err);
                        }
                    } else {
                        debug!("no hw change");
                    }
                } else {
                    debug!("leader is committed");
                }
                debug!("follower changed");
                true
            } else {
                false
            }
        } else {
            error!(follower_id, "invalid follower");
            false
        };

        drop(followers);

        self.notify_followers(notifier).await;
        if update {
            self.update_status().await;
        }

        update
    }

    /// compute follower that needs to be updated
    /// based on leader's state
    pub async fn follower_updates(
        &self,
        follower_id: &SpuId,
        max_bytes: u32,
    ) -> Option<PeerFileTopicResponse> {
        let leader_offset = self.as_offset();

        let reader = self.followers.read().await;
        if let Some(follower_info) = reader.get(follower_id) {
            if follower_info.is_valid() && !follower_info.is_same(&leader_offset) {
                let mut topic_response = PeerFileTopicResponse {
                    name: self.id().topic.to_owned(),
                    ..Default::default()
                };
                let mut partition_response = PeerFilePartitionResponse {
                    partition: self.id().partition,
                    ..Default::default()
                };

                // if this follower's leo is less than leader's leo then send diff
                if follower_info.leo < leader_offset.leo {
                    match self
                        .read_records(follower_info.leo, max_bytes, Isolation::ReadUncommitted)
                        .await
                    {
                        Ok(slice) => {
                            debug!(
                                hw = slice.end.hw,
                                leo = slice.end.leo,
                                replica = %self.id(),
                                "read records"
                            );
                            partition_response.hw = slice.end.hw;
                            partition_response.leo = slice.end.leo;
                            if let Some(file_slice) = slice.file_slice {
                                partition_response.records = file_slice.into();
                            }
                        }
                        Err(err) => {
                            error!(%err, "error reading records");
                            partition_response.error = err;
                        }
                    }
                } else {
                    // only hw need to be updated
                    debug!(
                        hw = leader_offset.hw,
                        leo = leader_offset.leo,
                        replica = %self.id(),
                        "sending hw only");
                }

                // ensure leo and hw are set correctly. storage might have update last stable offset
                partition_response.leo = leader_offset.leo;
                partition_response.hw = leader_offset.hw;

                topic_response.partitions.push(partition_response);
                Some(topic_response)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// convert myself as
    async fn as_lrs_request(&self) -> LrsRequest {
        let leader = (self.leader(), self.hw(), self.leo()).into();
        let replicas: Vec<ReplicaStatus> = self
            .followers
            .read()
            .await
            .iter()
            .map(|(follower_id, follower_info)| {
                (*follower_id, follower_info.hw, follower_info.leo).into()
            })
            .collect();
        let storage_reader = self.storage.read().await;
        let size = storage_reader
            .get_partition_size()
            .try_into()
            .unwrap_or(PartitionStatus::SIZE_ERROR);
        let base_offset = storage_reader.get_log_start_offset();

        LrsRequest::new(self.id().to_owned(), leader, replicas, size, base_offset)
    }

    #[instrument(skip(self))]
    pub async fn update_status(&self) {
        let lrs = self.as_lrs_request().await;
        debug!(
            hw = lrs.leader.hw,
            leo = lrs.leader.leo,
            size = lrs.size,
            base_offset = lrs.base_offset
        );
        self.status_update.send(lrs).await
    }

    /// write records to storage
    /// then update our follower's leo
    #[instrument(skip(self, records, notifiers))]
    pub async fn write_record_set(
        &self,
        records: &mut RecordSet<RawRecords>,
        notifiers: &FollowerNotifier,
    ) -> Result<(Offset, Offset, usize)> {
        self.transform(records).await?;
        if records.total_records() == 0 {
            return Ok((self.hw(), self.leo(), 0));
        }

        let offsets = self
            .storage
            .write_record_set(records, self.in_sync_replica == 1)
            .await?;

        self.notify_followers(notifiers).await;
        self.update_status().await;

        Ok(offsets)
    }

    async fn transform(&self, records: &mut RecordSet<RawRecords>) -> Result<()> {
        if let Some(ref sm_ctx) = self.sm_ctx {
            let (sm_result, sm_error) =
                process_record_set(sm_ctx.write().await.chain_mut(), records)?;
            if let Some(error) = sm_error {
                return Err(error.into());
            }
            records.batches.clear();
            if !sm_result.records().is_empty() {
                let transformed_batch = Batch::<RawRecords>::try_from(sm_result)?;
                records.batches.push(transformed_batch);
            }
        };
        Ok(())
    }

    async fn notify_followers(&self, notifier: &FollowerNotifier) {
        let leader_offset = self.as_offset();
        let followers = self.followers.read().await;
        debug!(?leader_offset);
        for follower in &self.replica.replicas {
            if let Some(follower_info) = followers.get(follower) {
                debug!(follower, ?follower_info);
                if follower_info.is_valid() && !follower_info.is_same(&leader_offset) {
                    debug!(follower, "notify");
                    notifier.notify_follower(follower, self.id().clone()).await;
                } else {
                    debug!(follower, "no update");
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn live_replicas(&self) -> Vec<SpuId> {
        self.followers.read().await.keys().cloned().collect()
    }

    // get copy of followers_info for debugging
    #[allow(unused)]
    pub async fn followers_info(&self) -> BTreeMap<SpuId, OffsetInfo> {
        self.followers.read().await.clone()
    }

    #[cfg(test)]
    pub fn consumer_offset_publishers(&self) -> Arc<Mutex<Vec<WeakSharedOffsetPublisher>>> {
        self.consumer_offset_publishers.clone()
    }

    pub async fn register_offset_publisher(&self, offset_publisher: &SharedOffsetPublisher) {
        let mut publishers = self.consumer_offset_publishers.lock().await;

        // Filter out any dead weak pointers every so often
        if publishers.len() % CLEANUP_FREQUENCY == 0 {
            let cleaned_publishers: Vec<WeakSharedOffsetPublisher> = publishers
                .iter()
                .filter_map(|p| p.upgrade())
                .map(|p| Arc::downgrade(&p))
                .collect();
            *publishers = cleaned_publishers;
        }

        let publisher = offset_publisher.clone();
        let publisher = Arc::downgrade(&publisher);

        publishers.push(publisher);
    }

    pub async fn signal_topic_deleted(&self) {
        let offset_publishers = self.consumer_offset_publishers.lock().await;

        for publisher in offset_publishers.iter() {
            if let Some(p) = publisher.upgrade() {
                p.update(TOPIC_DELETED);
            }
        }
    }

    /// append new record set.  this ensure record sets are aligned with leo
    /// if not aligned, it will return false
    pub(crate) async fn append_record_set(
        &self,
        records: &mut RecordSet<RawRecords>,
        notifier: &FollowerNotifier,
    ) -> Result<bool> {
        let total_records = records.total_records();
        debug!(total_records, "update from mirror source");
        if records.total_records() == 0 {
            debug!("no records");
            return Ok(false);
        }

        // verify that new records are aligned with current leo
        let storage_leo = self.leo();
        if records.base_offset() != storage_leo {
            return Ok(false);
        }

        self.write_record_set(records, notifier).await?;

        Ok(true)
    }
}

pub struct Uninit<S>(S);

impl<S: ReplicaStorage + 'static> Uninit<LeaderReplicaState<S>>
where
    S: Sync + Send,
{
    pub async fn init(self, ctx: &GlobalContext<FileReplica>) -> Result<LeaderReplicaState<S>> {
        let mut state = self.0;
        if let Some(dedup) = &state.replica.deduplication {
            debug!(?state.replica.deduplication, "init leader smartmodule context");
            let dedup_filter = dedup_to_invocation(dedup);
            let mut sm_ctx = SmartModuleContext::try_from(vec![dedup_filter], COMMON_VERSION, ctx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("SmartModule context is required here"))?;
            sm_ctx
                .look_back(&state)
                .await
                .context("leader smartmodule context lookback failed")?;
            state.sm_ctx = Some(Arc::new(RwLock::new(sm_ctx)));
        };
        // start up mirror controller if mirror is source
        if let Some(mirror) = &state.replica.mirror {
            match mirror {
                PartitionMirrorConfig::Remote(r) => {
                    debug!("found mirror remote, starting controller");
                    let mirror_controller_state = MirrorRemoteToHomeController::run(
                        ctx,
                        state.clone(),
                        r.clone(),
                        Isolation::ReadUncommitted,
                        10000000,
                    );
                    state.mirror_controller_state = Some(mirror_controller_state);
                }
                PartitionMirrorConfig::Home(_) => {
                    debug!("ignoring home for now");
                }
            }
        }
        Ok(state)
    }

    #[cfg(test)]
    pub(crate) fn into_inner(self) -> LeaderReplicaState<S> {
        self.0
    }
}

/// compute leader's updated hw based on follower offset
/// this is done after follower's leo updated
/// min_replica must be at least 1 and must be less than followers.len(0)
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
///  Simple HW mark calculation (assume LRS = 2) which is find minimum offset values that satisfy
///     Assume: Leader leo = 10, hw = 2,
///         follower: leo(2,4)  =>   no change, since it doesn't satisfy minim LRS
///         follower: leo(3,4)  =>   hw = 3  that is smallest leo that satisfy
///         follower: leo(4,4)  =>   hw = 4
///         follower: leo(6,7,9) =>  hw = 7,
fn compute_hw(
    leader: &OffsetInfo,
    min_replica: u16,
    followers: &BTreeMap<SpuId, OffsetInfo>,
) -> Option<Offset> {
    // assert!(min_replica > 0);
    // assert!((min_replica - 1) <= followers.len() as u16);
    let min_lrs = min((min_replica - 1) as usize, followers.len());

    // compute offsets that is greater than min leader's HW
    let mut qualified_leos_iter = followers
        .values()
        .map(|follower_info| follower_info.leo)
        .filter(|leo| *leo > leader.hw);

    if min_lrs == 0 {
        return qualified_leos_iter.max();
    }

    let mut qualified_leos: Vec<Reverse<Offset>> = Vec::new();
    for _ in 0..min_lrs {
        match qualified_leos_iter.next() {
            Some(leo) => qualified_leos.push(Reverse(leo)),
            None => return None,
        };
    }

    // sort with O(min_lrs*log(min_lrs)) time without extra memory
    let mut sorted_leos = BinaryHeap::from(qualified_leos);

    // insert and sort with O((n - min_lrs)*log(min_lrs)) in the worst case
    // without allocating memory
    for leo in qualified_leos_iter {
        if let Some(mut min) = sorted_leos.peek_mut() {
            if min.0 < leo {
                min.0 = leo;
            }
        }
    }
    sorted_leos.peek().map(|r| r.0)
}

impl<S> LeaderReplicaState<S> where S: ReplicaStorage {}

impl LeaderReplicaState<FileReplica> {}

#[cfg(test)]
mod test_hw_updates {

    use super::*;

    fn offsets_maps(offsets: Vec<(SpuId, OffsetInfo)>) -> BTreeMap<SpuId, OffsetInfo> {
        offsets.into_iter().collect()
    }

    /// test min lrs check
    /*
    #[test]
    #[should_panic]
    fn test_hw_min_lrs_invalid_hw() {
        compute_hw(&OffsetInfo { hw: 0, leo: 10 }, 0, &offsets_maps(vec![]));
    }
    */

    /*
    TODO: Revisit check of min lrs
    #[test]
    #[should_panic]
    fn test_hw_min_lrs_too_much() {
        compute_hw(
            &OffsetInfo { hw: 0, leo: 10 },
            3,
            &offsets_maps(vec![(5001, OffsetInfo::default())]),
        );
    }
    */
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

        // we take maximum leo since min lrs = 2
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

#[cfg(test)]
mod test_leader {

    use async_trait::async_trait;

    use fluvio_controlplane_metadata::partition::ReplicaKey;
    use fluvio_storage::{ReplicaStorage, ReplicaStorageConfig, OffsetInfo, ReplicaSlice};
    use fluvio_protocol::record::Offset;
    use fluvio_protocol::link::ErrorCode;
    use fluvio_protocol::record::BatchRecords;
    use fluvio_protocol::fixture::create_raw_recordset;

    use crate::config::SpuConfig;
    use crate::control_plane::StatusLrsMessageSink;

    use super::*;

    const MAX_BYTES: u32 = 1000;

    #[derive(Default)]
    struct MockConfig {}

    impl ReplicaStorageConfig for MockConfig {
        fn update_from_replica(&mut self, _replica: &Replica) {}
    }

    #[derive(Default)]
    struct MockStorage {
        pos: OffsetInfo,
    }

    impl From<&SpuConfig> for MockConfig {
        fn from(_log: &SpuConfig) -> MockConfig {
            MockConfig::default()
        }
    }

    #[async_trait]
    impl ReplicaStorage for MockStorage {
        async fn create_or_load(
            _replica: &ReplicaKey,
            _config: Self::ReplicaConfig,
        ) -> Result<Self> {
            Ok(MockStorage {
                pos: OffsetInfo { leo: 0, hw: 0 },
            })
        }

        fn get_hw(&self) -> Offset {
            self.pos.hw
        }

        fn get_leo(&self) -> Offset {
            self.pos.leo
        }

        async fn read_partition_slice(
            &self,
            offset: Offset,
            _max_len: u32,
            _isolation: Isolation,
        ) -> Result<ReplicaSlice, ErrorCode> {
            Ok(ReplicaSlice {
                end: OffsetInfo { leo: offset, hw: 0 },
                ..Default::default()
            })
        }

        // do dummy implementations of write
        async fn write_recordset<R: BatchRecords>(
            &mut self,
            records: &mut fluvio_protocol::record::RecordSet<R>,
            update_highwatermark: bool,
        ) -> Result<usize> {
            self.pos.leo = records.last_offset().unwrap();
            if update_highwatermark {
                self.pos.hw = self.pos.leo;
            }
            // assume 1 byte records
            Ok((self.pos.hw - self.pos.leo) as usize)
        }

        // just return hw multiplied by 100.
        fn get_partition_size(&self) -> u64 {
            (self.pos.hw * 100) as u64
        }

        async fn update_high_watermark(
            &mut self,
            offset: Offset,
        ) -> Result<bool, fluvio_storage::StorageError> {
            self.pos.hw = offset;
            Ok(true)
        }

        type ReplicaConfig = MockConfig;

        fn get_log_start_offset(&self) -> Offset {
            (self.pos.hw * 10) as Offset
        }

        async fn remove(&self) -> Result<(), fluvio_storage::StorageError> {
            todo!()
        }
    }

    #[fluvio_future::test]
    async fn test_leader_in_sync_replica() {
        let leader_config = SpuConfig {
            id: 5000,
            ..Default::default()
        };

        let replica: ReplicaKey = ("test", 1).into();
        // inserting new replica state, this should set follower offset to -1,-1 as initial state
        let state: LeaderReplicaState<MockStorage> = LeaderReplicaState::create(
            Replica::new(replica, 5000, vec![5000]),
            &leader_config,
            StatusLrsMessageSink::shared(),
        )
        .await
        .expect("state")
        .0;

        assert_eq!(state.in_sync_replica, 1);
    }

    #[fluvio_future::test]
    async fn test_follower_update() {
        let leader_config = SpuConfig {
            id: 5000,
            ..Default::default()
        };

        let notifier = FollowerNotifier::shared();

        let replica: ReplicaKey = ("test", 1).into();
        // inserting new replica state, this should set follower offset to -1,-1 as initial state
        let state: LeaderReplicaState<MockStorage> = LeaderReplicaState::create(
            Replica::new(replica, 5000, vec![5001, 5002]),
            &leader_config,
            StatusLrsMessageSink::shared(),
        )
        .await
        .expect("state")
        .0;

        // write fake recordset to ensure leo = 10
        state
            .write_record_set(&mut create_raw_recordset(10), &notifier)
            .await
            .expect("write");
        state.update_hw(2).await.expect("hw");

        assert_eq!(state.leo(), 10);
        assert_eq!(state.hw(), 2);

        let follower_info = state.followers.read().await;
        assert!(!follower_info.get(&5001).unwrap().is_valid()); // follower should be invalid sate;
        drop(follower_info);

        assert!(state.follower_updates(&5003, MAX_BYTES).await.is_none()); // don't have 5003
        assert!(state.follower_updates(&5001, MAX_BYTES).await.is_none()); // 5001 is still invalid
        assert!(state.follower_updates(&5002, MAX_BYTES).await.is_none()); // 5002 is still invalid

        // got updated from 5001 which just been initialized
        let mut followers = state.followers.write().await;
        followers
            .get_mut(&5001)
            .expect("map")
            .update(&OffsetInfo { leo: 0, hw: 0 });
        drop(followers);

        assert!(state.follower_updates(&5002, MAX_BYTES).await.is_none()); // 5002 is still invalid
        let updates = state
            .follower_updates(&5001, MAX_BYTES)
            .await
            .expect("some");
        assert_eq!(updates.name, "test");
        assert_eq!(updates.partitions[0].leo, 10);
        assert_eq!(updates.partitions[0].hw, 2);

        // updated from 5002
        let mut followers = state.followers.write().await;
        followers
            .get_mut(&5002)
            .expect("map")
            .update(&OffsetInfo { leo: 0, hw: 0 });
        drop(followers);
        let updates = state
            .follower_updates(&5002, MAX_BYTES)
            .await
            .expect("some");
        assert_eq!(updates.name, "test");
        assert_eq!(updates.partitions[0].leo, 10);
        assert_eq!(updates.partitions[0].hw, 2);

        // 5002 has been fully caught up
        let mut followers = state.followers.write().await;
        followers
            .get_mut(&5002)
            .expect("map")
            .update(&OffsetInfo { leo: 10, hw: 2 });
        drop(followers);
        assert!(state.follower_updates(&5002, MAX_BYTES).await.is_none()); // 5002 is still invalid
        assert!(state.follower_updates(&5001, MAX_BYTES).await.is_some()); // 5001 is still need to besync
    }

    #[fluvio_future::test]
    async fn test_update_leader_from_followers() {
        use crate::core::GlobalContext;
        use fluvio_controlplane_metadata::spu::SpuSpec;

        let leader_config = SpuConfig {
            id: 5000,
            ..Default::default()
        };
        let specs = vec![
            SpuSpec::new_private_addr(5000, 9000, "localhost".to_owned()),
            SpuSpec::new_private_addr(5001, 9001, "localhost".to_owned()),
            SpuSpec::new_private_addr(5002, 9002, "localhost".to_owned()),
        ];
        let gctx: Arc<GlobalContext<MockStorage>> =
            GlobalContext::new_shared_context(leader_config);
        gctx.spu_localstore().sync_all(specs);
        gctx.sync_follower_update().await;

        let notifier = gctx.follower_notifier();
        assert!(notifier.get(&5001).await.is_some());
        assert!(notifier.get(&5002).await.is_some());
        assert!(notifier.get(&5000).await.is_none());

        let replica: ReplicaKey = ("test", 1).into();
        // inserting new replica state, this should set follower offset to -1,-1 as initial state
        let leader: LeaderReplicaState<MockStorage> = LeaderReplicaState::create(
            Replica::new(replica.clone(), 5000, vec![5000, 5001, 5002]),
            gctx.config(),
            StatusLrsMessageSink::shared(),
        )
        .await
        .expect("state")
        .0;

        // follower's offset should be init
        let follower_info = leader.followers_info().await;
        assert_eq!(follower_info.get(&5001).unwrap().leo, -1);
        assert_eq!(follower_info.get(&5001).unwrap().hw, -1);

        let f1 = notifier.get(&5001).await.expect("5001");
        let f2 = notifier.get(&5002).await.expect("5002");

        // write fake recordset to ensure leo = 10
        leader
            .write_record_set(&mut create_raw_recordset(10), notifier)
            .await
            .expect("write");

        // check leader leo = 10 and hw = 2
        assert_eq!(leader.leo(), 10);
        assert_eq!(leader.hw(), 0);
        assert!(f1.drain_replicas().await.is_empty());
        assert!(f2.drain_replicas().await.is_empty());

        // handle invalidate offset update from follower
        assert!(
            !leader
                .update_states_from_followers(5001, OffsetInfo { leo: 5, hw: 20 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);
        assert!(f1.drain_replicas().await.is_empty());

        // update from invalid follower
        assert!(
            !leader
                .update_states_from_followers(5004, OffsetInfo { leo: 6, hw: 11 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);

        // handle newer leo
        assert!(
            !leader
                .update_states_from_followers(5001, OffsetInfo { leo: 20, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);
        assert!(!f1.has_replica(&replica).await); // no update to follower required

        debug!(offsets = ?leader.followers_info().await,"updating 5001 with leo=0,hw=0");
        assert!(
            leader
                .update_states_from_followers(5001, OffsetInfo { leo: 0, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0); // no change on hw since we just updated the update true follower's state
        assert!(f1.drain_replicas().await.contains(&replica));
        //  debug!(f2 = ?f2.drain_replicas().await);
        assert!(f2.drain_replicas().await.is_empty()); // f2 is still invalid

        // 5001 partial update, follower still need to sync up with leader
        debug!(offsets = ?leader.followers_info().await,"updating 5001 with leo=6,hw=0");
        assert!(
            leader
                .update_states_from_followers(5001, OffsetInfo { leo: 6, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);
        assert!(f1.drain_replicas().await.contains(&replica));

        // 5001 has fully caught up with leader, nothing to update followers until 5002 has update
        debug!(offsets = ?leader.followers_info().await,"updating 5001 with leo=10,hw=0");
        assert!(
            leader
                .update_states_from_followers(5001, OffsetInfo { leo: 10, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);
        assert!(f1.drain_replicas().await.is_empty());
        assert!(f2.drain_replicas().await.is_empty());
        let follower_info = leader.followers_info().await;
        assert_eq!(follower_info.get(&5001).unwrap().leo, 10);
        assert_eq!(follower_info.get(&5001).unwrap().hw, 0);

        // init 5002
        debug!(offsets = ?leader.followers_info().await,"updating 5002 with leo=0,hw=0");
        assert!(
            leader
                .update_states_from_followers(5002, OffsetInfo { leo: 0, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 0);
        assert!(f2.drain_replicas().await.contains(&replica));

        // partial update of 5002, this lead hw to 6, both followers will be updated
        debug!(offsets = ?leader.followers_info().await,"updating 5002 with leo=6,hw=0");
        assert!(
            leader
                .update_states_from_followers(5002, OffsetInfo { leo: 6, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 6);
        assert!(f1.drain_replicas().await.contains(&replica));
        assert!(f2.drain_replicas().await.contains(&replica));

        // 5002 full update, both followers will be updated
        debug!(offsets = ?leader.followers_info().await,"updating 5002 with leo=10,hw=0");
        assert!(
            leader
                .update_states_from_followers(5002, OffsetInfo { leo: 10, hw: 0 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 10);
        assert!(f1.drain_replicas().await.contains(&replica));
        assert!(f2.drain_replicas().await.contains(&replica));

        // 5002 same update, 5001 still need update
        debug!(offsets = ?leader.followers_info().await,"updating 5002 with leo=10,hw=10");
        assert!(
            leader
                .update_states_from_followers(5002, OffsetInfo { leo: 10, hw: 10 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 10);
        assert!(f1.drain_replicas().await.contains(&replica));
        assert!(f2.drain_replicas().await.is_empty());

        // 5001 is now same as both leader and 5002
        debug!(offsets = ?leader.followers_info().await,"updating 5001 with leo=10,hw=10");
        assert!(
            leader
                .update_states_from_followers(5001, OffsetInfo { leo: 10, hw: 10 }, notifier)
                .await
        );
        assert_eq!(leader.hw(), 10);
        assert!(f1.drain_replicas().await.is_empty());
        assert!(f2.drain_replicas().await.is_empty());
    }
}
