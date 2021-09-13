use std::ops::Deref;
use std::sync::{RwLock, Arc};
use std::collections::HashMap;

use tracing::{error, instrument};

use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use fluvio_storage::{FileReplica, StorageError};

use crate::core::GlobalContext;
use crate::config::ReplicationConfig;
use crate::replication::follower::FollowerReplicaState;

use super::{LeaderReplicaState, replica_state::SharedLeaderState};
use crate::control_plane::StatusMessageSink;

/// Collection of replicas
#[derive(Debug)]
pub struct ReplicaLeadersState<S>(RwLock<HashMap<ReplicaKey, SharedLeaderState<S>>>);

impl<S> Default for ReplicaLeadersState<S> {
    fn default() -> Self {
        ReplicaLeadersState(RwLock::new(HashMap::new()))
    }
}

impl<S> Deref for ReplicaLeadersState<S> {
    type Target = RwLock<HashMap<ReplicaKey, SharedLeaderState<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> ReplicaLeadersState<S> {
    pub fn new_shared() -> ReplicaLeadersState<S> {
        Self::default()
    }
}

impl<S> ReplicaLeadersState<S> {
    /// get clone of state
    pub fn get(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let read = self.read().unwrap();
        read.get(replica).cloned()
    }

    pub fn remove(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().unwrap();
        writer.remove(replica)
    }

    #[allow(unused)]
    pub fn insert(
        &self,
        replica: ReplicaKey,
        state: SharedLeaderState<S>,
    ) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().unwrap();
        writer.insert(replica, state)
    }
}

impl ReplicaLeadersState<FileReplica> {
    #[instrument(
        skip(self, ctx,replica,status_update),
        fields(replica = %replica.id)
    )]
    pub async fn add_leader_replica(
        &self,
        ctx: &GlobalContext<FileReplica>,
        replica: Replica,
        status_update: Arc<StatusMessageSink>,
    ) -> Result<LeaderReplicaState<FileReplica>, StorageError> {
        let replica_id = replica.id.clone();

        let leader_replica =
            LeaderReplicaState::create(replica, ctx.config(), status_update).await?;
        self.insert_leader(replica_id, leader_replica.clone()).await;
        Ok(leader_replica)
    }

    #[instrument(
        skip(self,replica_id, leader_state),
        fields(replica = %replica_id)
    )]
    async fn insert_leader(
        &self,
        replica_id: ReplicaKey,
        leader_state: LeaderReplicaState<FileReplica>,
    ) {
        let mut writer = self.write().unwrap();
        if let Some(old_replica) = writer.insert(replica_id.clone(), leader_state.clone()) {
            error!(
                "there was existing replica when creating new leader replica: {}",
                old_replica.id()
            );
        }
    }

    /// promote follower
    #[instrument(
        skip(self,follower,replica,status_update),
        fields(replica = %replica.id)
    )]
    pub async fn promote_follower(
        &self,
        config: ReplicationConfig,
        follower: FollowerReplicaState<FileReplica>,
        replica: Replica,
        status_update: Arc<StatusMessageSink>,
    ) -> LeaderReplicaState<FileReplica> {
        let replica_id = replica.id.clone();
        let replica_storage = follower.inner_owned();
        let leader = LeaderReplicaState::new(replica, config, status_update, replica_storage);
        self.insert_leader(replica_id, leader.clone()).await;
        leader
    }
}
