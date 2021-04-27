//!
//! # Global Context
//!
//! Global Context maintains states need to be shared across in the SPU
use std::sync::Arc;
use std::fmt::Debug;

use fluvio_types::SpuId;
use fluvio_storage::ReplicaStorage;

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

    // sync follower pending updates with
    pub async fn sync_follower_update(&self) {
        self.spu_followers
            .sync_from_spus(self.spu_localstore(), self.local_spu_id())
            .await;
    }
}
