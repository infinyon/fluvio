use std::{fmt::Display, sync::Arc};
use std::fmt::Debug;
use std::collections::{HashMap, hash_map::Entry};
use std::ops::{Deref, DerefMut};

use fluvio_controlplane::replica::Replica;
use tracing::{debug, warn, instrument};
use tokio::sync::RwLock;
use anyhow::Result;

use fluvio_protocol::record::{BatchRecords, ReplicaKey};
use fluvio_storage::config::ReplicaConfig;
use fluvio_protocol::record::RecordSet;
use fluvio_protocol::record::Offset;
use fluvio_storage::{FileReplica, ReplicaStorage, ReplicaStorageConfig};
use fluvio_types::SpuId;

use crate::replication::leader::ReplicaOffsetRequest;
use crate::core::FileGlobalContext;
use crate::storage::SharableReplicaStorage;

use super::controller::FollowerGroups;

pub type SharedFollowersState<S> = Arc<FollowersState<S>>;

/// Maintains state for followers
/// Each follower controller maintains by SPU
#[derive(Debug)]
pub struct FollowersState<S> {
    states: RwLock<HashMap<ReplicaKey, FollowerReplicaState<S>>>,
    groups: FollowerGroups,
}

impl<S> Deref for FollowersState<S> {
    type Target = RwLock<HashMap<ReplicaKey, FollowerReplicaState<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.states
    }
}

impl<S> FollowersState<S>
where
    S: ReplicaStorage,
{
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self {
            states: RwLock::new(HashMap::new()),
            groups: FollowerGroups::new(),
        })
    }

    pub async fn get(&self, replica: &ReplicaKey) -> Option<FollowerReplicaState<S>> {
        let read = self.read().await;
        read.get(replica).cloned()
    }

    pub async fn followers_by_spu(
        &self,
        spu: SpuId,
    ) -> HashMap<ReplicaKey, FollowerReplicaState<S>> {
        let mut replicas = HashMap::new();
        let read = self.read().await;
        for (key, state) in read.iter() {
            if state.leader() == spu {
                replicas.insert(key.clone(), state.clone());
            }
        }
        replicas
    }
}

impl FollowersState<FileReplica> {
    /// try to add new replica
    /// if there isn't existing spu group, create new one and return new replica
    /// otherwise check if there is existing state, if exists return none otherwise create new
    pub async fn add_replica(
        self: Arc<Self>,
        ctx: &FileGlobalContext,
        replica: Replica,
    ) -> Result<Option<FollowerReplicaState<FileReplica>>> {
        let leader = replica.leader;

        let mut writer = self.write().await;
        match writer.entry(replica.id.clone()) {
            Entry::Occupied(_) => {
                // follower exists, nothing to do
                warn!(%replica, "replica already exists");
                Ok(None)
            }
            Entry::Vacant(entry) => {
                debug!(
                    replica = %replica.id,
                    "creating new follower state"
                );

                let mut replica_config: ReplicaConfig = ctx.config().into();
                replica_config.update_from_replica(&replica);

                let replica_state =
                    FollowerReplicaState::create(leader, replica.id, replica_config).await?;

                entry.insert(replica_state.clone());
                self.groups.check_new(ctx, leader).await;
                Ok(Some(replica_state))
            }
        }
    }

    /// remove replica
    /// if there are no more replicas per leader
    /// then we shutdown controller
    #[instrument(skip(self))]
    pub async fn remove_replica(
        &self,
        leader: SpuId,
        key: &ReplicaKey,
    ) -> Option<FollowerReplicaState<FileReplica>> {
        let mut writer = self.write().await;
        if let Some(replica) = writer.remove(key) {
            debug!("removed follower");

            // find out how many replicas by leader
            let replica_count = writer
                .values()
                .filter(|rep_ref| rep_ref.leader() == leader)
                .count();

            debug!(replica_count, leader, "new replica count");

            if replica_count == 0 {
                self.groups.remove(leader).await;
            } else {
                self.groups.update(leader).await;
            }
            Some(replica)
        } else {
            None
        }
    }

    pub async fn update_replica(&self, _replica: Replica) {}
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
        config: S::ReplicaConfig,
    ) -> Result<Self>
    where
        S::ReplicaConfig: Display,
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
    pub async fn update_from_leader<R: BatchRecords>(
        &self,
        records: &mut RecordSet<R>,
        leader_hw: Offset,
    ) -> Result<bool> {
        let mut changes = false;

        if records.total_records() > 0 {
            self.write_recordsets(records).await?;
            changes = true;
        } else {
            debug!("no records");
        }

        let f_offset = self.as_offset();
        // update hw assume it's valid
        if f_offset.hw != leader_hw {
            if f_offset.hw > leader_hw {
                warn!(
                    follower_hw = f_offset.hw,
                    leader_hw, "leader hw is less than hw"
                );
            } else if leader_hw > f_offset.leo {
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

        Ok(changes)
    }

    /// try to write records
    /// ensure records has correct baseoffset
    async fn write_recordsets<R: BatchRecords>(&self, records: &mut RecordSet<R>) -> Result<bool> {
        let storage_leo = self.leo();
        if records.base_offset() != storage_leo {
            // this could happened if records were sent from leader before hw was sync
            warn!(
                storage_leo,
                incoming_base_offset = records.base_offset(),
                "follower leo is not same as base offset, skipping write"
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

    use flv_util::fixture::ensure_clean_dir;
    use fluvio_types::{SpuId, PartitionId};
    use fluvio_storage::config::ReplicaConfig;

    use super::*;

    const LEADER: SpuId = 5001;
    const TOPIC: &str = "test";
    const TEST_REPLICA: (&str, PartitionId) = (TOPIC, 0);

    #[fluvio_future::test]
    async fn test_follower_creation() {
        let test_path = "/tmp/follower_init";
        ensure_clean_dir(test_path);

        let config = ReplicaConfig {
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
    }
}
