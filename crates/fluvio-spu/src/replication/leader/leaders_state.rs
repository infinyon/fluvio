use std::ops::Deref;
use async_lock::RwLock;
use fluvio_controlplane::replica::Replica;
use std::collections::HashMap;

use tracing::{error, instrument};
use anyhow::Result;

use fluvio_controlplane_metadata::partition::{PartitionMirrorConfig, ReplicaKey};
use fluvio_storage::{FileReplica, ReplicaStorage};

use crate::{control_plane::SharedLrsStatusUpdate, core::GlobalContext};
use crate::config::ReplicationConfig;
use crate::replication::follower::FollowerReplicaState;

use super::{LeaderReplicaState, replica_state::SharedLeaderState};

pub type SharedReplicaLeadersState<S> = ReplicaLeadersState<S>;

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
    pub fn new_shared() -> SharedReplicaLeadersState<S> {
        Self::default()
    }
}

impl<S> ReplicaLeadersState<S> {
    /// get clone of state
    pub async fn get(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let read = self.read().await;
        read.get(replica).cloned()
    }

    pub async fn remove(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().await;
        writer.remove(replica)
    }

    #[allow(unused)]
    pub async fn insert(
        &self,
        replica: ReplicaKey,
        state: SharedLeaderState<S>,
    ) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().await;
        writer.insert(replica, state)
    }
}

impl<S> ReplicaLeadersState<S>
where
    S: ReplicaStorage,
{
    /// find all replica configs
    #[cfg(test)]
    pub(crate) async fn replica_configs(&self) -> Vec<Replica> {
        let read = self.read().await;

        let mut replicas = Vec::new();
        for (_replica_key, state) in read.iter() {
            let replica_config = state.get_replica();
            replicas.push(replica_config.clone());
        }
        replicas
    }

    /// find replica with mirror target that matches remote cluster and sourcre replica
    /// also return if it is source or target
    pub(crate) async fn find_mirror_home_leader(
        &self,
        remote_cluster: &str,
        home_replica: &str,
    ) -> Option<(SharedLeaderState<S>, bool)> {
        let read = self.read().await;
        for (_replica_key, state) in read.iter() {
            let replica_config = state.get_replica();
            if let Some(PartitionMirrorConfig::Home(home)) = &replica_config.mirror {
                if home.remote_cluster == remote_cluster && home.remote_replica == home_replica {
                    return Some((state.clone(), home.source));
                }
            }
        }
        None
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
        status_update: SharedLrsStatusUpdate,
    ) -> Result<LeaderReplicaState<FileReplica>> {
        let replica_id = replica.id.clone();

        let leader_replica =
            LeaderReplicaState::create(replica, ctx.config(), status_update).await?;
        let leader_replica = leader_replica.init(ctx).await?;
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
        let mut writer = self.write().await;
        if let Some(old_replica) = writer.insert(replica_id.clone(), leader_state.clone()) {
            error!(
                "there was existing replica when creating new leader replica: {}",
                old_replica.id()
            );
        }
    }

    /// promote follower
    #[instrument(
        skip(self,follower,replica,status_update,ctx),
        fields(replica = %replica.id)
    )]
    pub async fn promote_follower(
        &self,
        config: ReplicationConfig,
        follower: FollowerReplicaState<FileReplica>,
        replica: Replica,
        status_update: SharedLrsStatusUpdate,
        ctx: &GlobalContext<FileReplica>,
    ) -> Result<LeaderReplicaState<FileReplica>> {
        let replica_id = replica.id.clone();
        let replica_storage = follower.inner_owned();
        let leader = LeaderReplicaState::new(replica, config, status_update, replica_storage);
        let leader = leader.init(ctx).await?;
        self.insert_leader(replica_id, leader.clone()).await;
        Ok(leader)
    }
}
