//!
//! # Global Context
//!
//! Global Context maintains states need to be shared across in the SPU
use std::sync::Arc;
use std::fmt::Debug;

use tracing::{debug, error, instrument};

use fluvio_controlplane_metadata::partition::Replica;
use fluvio_types::SpuId;
use fluvio_storage::{FileReplica, ReplicaStorage};

use crate::config::SpuConfig;
use crate::replication::follower::FollowersState;
use crate::replication::follower::SharedFollowersState;
use crate::replication::leader::{
    SharedReplicaLeadersState, ReplicaLeadersState, FollowerNotifier, SharedSpuUpdates,
};
use crate::services::public::StreamPublishers;
use crate::control_plane::{StatusMessageSink, SharedStatusUpdate};

use super::spus::SharedSpuLocalStore;
use super::SharedReplicaLocalStore;
use super::spus::SpuLocalStore;
use super::replica::ReplicaStore;
use super::SharedSpuConfig;

#[derive(Debug)]
pub struct GlobalContext<S> {
    config: SharedSpuConfig,
    spu_localstore: SharedSpuLocalStore,
    replica_localstore: SharedReplicaLocalStore,
    leaders_state: SharedReplicaLeadersState<S>,
    followers_state: SharedFollowersState<S>,
    stream_publishers: StreamPublishers,
    spu_followers: SharedSpuUpdates,
    status_update: SharedStatusUpdate,
}

// -----------------------------------
// Global Contesxt - Implementation
// -----------------------------------

impl<S> GlobalContext<S>
where
    S: ReplicaStorage,
{
    pub fn new_shared_context(spu_config: SpuConfig) -> Arc<Self> {
        Arc::new(GlobalContext::new(spu_config))
    }

    pub fn new(spu_config: SpuConfig) -> Self {
        GlobalContext {
            spu_localstore: SpuLocalStore::new_shared(),
            replica_localstore: ReplicaStore::new_shared(),
            config: Arc::new(spu_config),
            leaders_state: ReplicaLeadersState::new_shared(),
            followers_state: FollowersState::new_shared(),
            stream_publishers: StreamPublishers::new(),
            spu_followers: FollowerNotifier::shared(),
            status_update: StatusMessageSink::shared(),
        }
    }

    pub fn spu_localstore_owned(&self) -> SharedSpuLocalStore {
        self.spu_localstore.clone()
    }

    /// retrieves local spu id
    pub fn local_spu_id(&self) -> SpuId {
        self.config.id
    }

    pub fn spu_localstore(&self) -> &SpuLocalStore {
        &self.spu_localstore
    }

    pub fn replica_localstore(&self) -> &ReplicaStore {
        &self.replica_localstore
    }

    pub fn leaders_state(&self) -> &ReplicaLeadersState<S> {
        &self.leaders_state
    }

    pub fn followers_state(&self) -> &FollowersState<S> {
        &self.followers_state
    }

    pub fn followers_state_owned(&self) -> SharedFollowersState<S> {
        self.followers_state.clone()
    }

    pub fn config(&self) -> &SpuConfig {
        &self.config
    }

    pub fn config_owned(&self) -> SharedSpuConfig {
        self.config.clone()
    }

    pub fn stream_publishers(&self) -> &StreamPublishers {
        &self.stream_publishers
    }

    pub fn follower_notifier(&self) -> &FollowerNotifier {
        &self.spu_followers
    }

    #[allow(unused)]
    pub fn status_update(&self) -> &StatusMessageSink {
        &self.status_update
    }

    pub fn status_update_owned(&self) -> SharedStatusUpdate {
        self.status_update.clone()
    }

    /// notify all follower handlers with SPU changes
    pub async fn sync_follower_update(&self) {
        self.spu_followers
            .sync_from_spus(self.spu_localstore(), self.local_spu_id())
            .await;
    }
}

impl GlobalContext<FileReplica> {
    /// Promote follower replica as leader,
    /// This is done in 3 steps
    /// // 1: Remove follower replica from followers state
    /// // 2: Terminate followers controller if need to be (if there are no more follower replicas for that controller)
    /// // 3: add to leaders state
    #[instrument(
        skip(self,new_replica,old_replica),
        fields(
            replica = %new_replica.id,
            old_leader = old_replica.leader
        )
    )]
    pub async fn promote(&self, new_replica: &Replica, old_replica: &Replica) {
        if let Some(follower_replica) = self
            .followers_state()
            .remove_replica(old_replica.leader, &old_replica.id)
            .await
        {
            debug!(
                replica = %old_replica.id,
                "old follower replica exists, promoting to leader"
            );

            self.leaders_state()
                .promote_follower(
                    self.config().into(),
                    follower_replica,
                    new_replica.clone(),
                    self.status_update_owned(),
                )
                .await;
        } else {
            error!("follower replica {} didn't exists!", old_replica.id);
        }
    }
}
