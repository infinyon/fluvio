use std::{fmt::Display, sync::Arc};
use std::fmt::Debug;
use std::collections::{HashMap};

use std::ops::{Deref, DerefMut};

use tracing::{debug, warn, error};
use async_rwlock::{RwLock};
use dashmap::DashMap;

use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use dataplane::record::RecordSet;
use dataplane::Offset;
use fluvio_storage::{FileReplica, StorageError, ReplicaStorage};
use fluvio_types::SpuId;
use fluvio_types::event::offsets::OffsetPublisher;
use crate::replication::leader::ReplicaOffsetRequest;
use crate::replication::follower::ReplicaFollowerController;
use crate::core::DefaultSharedGlobalContext;
use crate::storage::SharableReplicaStorage;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;

/// Maintains state for followers
/// Each follower controller maintains by SPU
#[derive(Debug)]
pub struct FollowersState<S> {
    states: DashMap<ReplicaKey, FollowerReplicaState<S>>,
    leaders: RwLock<HashMap<SpuId, Arc<FollowersBySpu>>>,
}

impl<S> Deref for FollowersState<S> {
    type Target = DashMap<ReplicaKey, FollowerReplicaState<S>>;

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
            leaders: RwLock::new(HashMap::new()),
        })
    }
}

impl FollowersState<FileReplica> {
    /// try to add new replica
    /// if there isn't existing spu group, create new one and return new replica
    /// otherwise check if there is existing state, if exists return none otherwise create new
    pub async fn add_replica(
        self: Arc<Self>,
        ctx: DefaultSharedGlobalContext,
        replica: Replica,
    ) -> Result<Option<FollowerReplicaState<FileReplica>>, StorageError> {
        let leader = replica.leader;

        if self.states.contains_key(&replica.id) {
            // follower exists, nothing to do
            warn!(%replica,"replica already exists");
            Ok(None)
        } else {
            debug!(
                "no existing follower controller exists for {},need to spin up",
                replica
            );

            let replica_state =
                FollowerReplicaState::create(leader, replica.id.clone(), ctx.config().into())
                    .await?;
            self.states.insert(replica.id, replica_state.clone());

            let mut leaders = self.leaders.write().await;
            // check if we have controllers
            if let Some(leaders) = leaders.get(&leader) {
                leaders.sync();
            } else {
                // don't have leader, so we need to create
                let followers_spu = FollowersBySpu::shared(leader);
                leaders.insert(leader, followers_spu.clone());

                ReplicaFollowerController::run(
                    leader,
                    ctx.spu_localstore_owned(),
                    self.clone(),
                    followers_spu.clone(),
                    ctx.config_owned(),
                );
            }

            Ok(Some(replica_state))
        }
    }

    /// remove replica
    /// if there are no more replicas per leader
    /// then we shutdown controller
    pub async fn remove_replica(
        &self,
        leader: SpuId,
        key: &ReplicaKey,
    ) -> Option<FollowerReplicaState<FileReplica>> {
        if let Some((_key, replica)) = self.remove(key) {
            let mut leaders = self.leaders.write().await;

            let replica_count = self
                .states
                .iter()
                .filter(|rep_ref| rep_ref.value().leader() == leader)
                .count();

            debug!(replica_count, leader, "new replica count");

            if replica_count == 0 {
                if let Some(old_leader) = leaders.remove(&leader) {
                    debug!(leader, "more more replicas, shutting down");
                    old_leader.shutdown();
                } else {
                    error!(leader, "was not founded");
                }
            } else {
                if let Some(old_leader) = leaders.get(&leader) {
                    debug!(leader, "resync");
                    old_leader.sync();
                } else {
                    error!(leader, "was not founded");
                }
            }
            Some(replica)
        } else {
            None
        }
    }

    pub async fn update_replica(&self, _replica: Replica) {}
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

    /// update count by 1 to force controller to re-compute replicas in it's holding
    pub fn sync(&self) {
        let last_value = self.events.current_value();
        self.events.update(last_value + 1);
    }

    pub fn shutdown(&self) {
        self.events.update(-1);
    }
}

/// State for Follower Replica Controller
/// This can be cloned
#[derive(Debug)]
pub struct FollowerReplicaState<S> {
    leader: SpuId,
    inner: SharableReplicaStorage<S>,
}

impl<S> Clone for FollowerReplicaState<S> {
    fn clone(&self) -> Self {
        Self {
            leader: self.leader,
            inner: self.inner.clone(),
        }
    }
}

impl<S> Deref for FollowerReplicaState<S> {
    type Target = SharableReplicaStorage<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> DerefMut for FollowerReplicaState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S> FollowerReplicaState<S>
where
    S: ReplicaStorage,
{
    pub async fn create(
        leader: SpuId,
        replica_key: ReplicaKey,
        config: S::Config,
    ) -> Result<Self, StorageError>
    where
        S::Config: Display,
    {
        debug!(
            %replica_key,
            leader,
            %config,
            "created follower replica"
        );

        let replica_storage = SharableReplicaStorage::create(replica_key, config).await?;

        Ok(Self {
            leader,
            inner: replica_storage,
        })
    }

    pub fn leader(&self) -> SpuId {
        self.leader
    }

    /// update from leader with new record set
    pub async fn update_from_leader(
        &self,
        records: &mut RecordSet,
        leader_hw: Offset,
    ) -> Result<bool, StorageError> {
        let mut changes = false;
        let f_offset = self.as_offset();

        if records.total_records() > 0 {
            self.write_recordsets(records).await?;
            changes = true;
        } else {
            debug!("no records");
        }

        // update hw assume it's valid
        if f_offset.hw != leader_hw {
            if f_offset.hw > leader_hw {
                warn!(
                    follower_hw = f_offset.hw,
                    leader_hw, "leader hw is less than hw"
                );
            } else {
                if leader_hw > f_offset.leo {
                    warn!(
                        leader_hw,
                        follower_leo = f_offset.leo,
                        "leade hw is greater than follower leo"
                    )
                } else {
                    debug!(f_offset.hw, leader_hw, "updating hw");
                    self.update_hw(leader_hw).await?;
                    changes = true;
                }
            }
        }

        Ok(changes)
    }

    /// try to write records
    /// ensure records has correct baseoffset
    async fn write_recordsets(&self, records: &mut RecordSet) -> Result<bool, StorageError> {
        let storage_leo = self.leo();
        if records.base_offset() != storage_leo {
            warn!(
                storage_leo,
                incoming_base_offset = records.base_offset(),
                "storage leo is not same as base offset"
            );
            Ok(false)
        } else {
            self.write_record_set(records, false).await?;
            Ok(true)
        }
    }

    /// convert to offset request
    pub fn as_offset_request(&self) -> ReplicaOffsetRequest {
        ReplicaOffsetRequest {
            replica: self.inner.id().to_owned(),
            leo: self.leo(),
            hw: self.hw(),
        }
    }

    pub fn inner_owned(self) -> SharableReplicaStorage<S> {
        self.inner
    }
}

#[cfg(test)]
mod follower_tests {

    use std::path::PathBuf;

    use fluvio_future::{test_async};
    use flv_util::fixture::ensure_clean_dir;
    use fluvio_types::SpuId;
    use fluvio_storage::config::ConfigOption;

    use super::*;

    const LEADER: SpuId = 5001;
    const TOPIC: &str = "test";
    const TEST_REPLICA: (&str, i32) = (TOPIC, 0);

    #[test_async]
    async fn test_follower_creation() -> Result<(), ()> {
        let test_path = "/tmp/follower_init";
        ensure_clean_dir(test_path);

        let config = ConfigOption {
            base_dir: PathBuf::from(test_path).join("spu-5002"),
            ..Default::default()
        };

        let follower_replica: FollowerReplicaState<FileReplica> =
            FollowerReplicaState::create(LEADER, TEST_REPLICA.into(), config)
                .await
                .expect("create");

        // at this point, follower replica should be empty since we don't have time to sync up with leader
        assert_eq!(follower_replica.leo(), 0);
        assert_eq!(follower_replica.hw(), 0);
        assert!(PathBuf::from(test_path).join("spu-5002").exists());

        Ok(())
    }
}
