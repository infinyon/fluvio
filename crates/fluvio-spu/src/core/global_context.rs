//!
//! # Global Context
//!
//! Global Context maintains states need to be shared across in the SPU

use std::sync::Arc;
use std::fmt::Debug;

use fluvio_smartengine::SmartEngine;
use tracing::{debug, error, instrument};

use fluvio_controlplane_metadata::partition::Replica;
use fluvio_types::SpuId;
use fluvio_storage::{ReplicaStorage};

use crate::config::SpuConfig;
use crate::replication::follower::FollowersState;
use crate::replication::follower::SharedFollowersState;
use crate::replication::leader::{
    SharedReplicaLeadersState, ReplicaLeadersState, FollowerNotifier, SharedSpuUpdates,
};
use crate::control_plane::{StatusMessageSink, SharedStatusUpdate};
use crate::core::metrics::SpuMetrics;

use super::leader_client::LeaderConnections;
use super::smartmodule::SmartModuleLocalStore;
use super::spus::SharedSpuLocalStore;
use super::SharedReplicaLocalStore;
use super::smartmodule::SharedSmartModuleLocalStore;
use super::spus::SpuLocalStore;
use super::replica::ReplicaStore;
use super::SharedSpuConfig;

pub use file_replica::ReplicaChange;

#[derive(Debug)]
pub struct GlobalContext<S> {
    config: SharedSpuConfig,
    spu_localstore: SharedSpuLocalStore,
    replica_localstore: SharedReplicaLocalStore,
    smartmodule_localstore: SharedSmartModuleLocalStore,
    leaders_state: SharedReplicaLeadersState<S>,
    followers_state: SharedFollowersState<S>,
    spu_followers: SharedSpuUpdates,
    status_update: SharedStatusUpdate,
    sm_engine: SmartEngine,
    leaders: Arc<LeaderConnections>,
    metrics: Arc<SpuMetrics>,
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
        let spus = SpuLocalStore::new_shared();
        let replicas = ReplicaStore::new_shared();
        let metrics = Arc::new(SpuMetrics::new());

        GlobalContext {
            spu_localstore: spus.clone(),
            replica_localstore: replicas.clone(),
            smartmodule_localstore: SmartModuleLocalStore::new_shared(),
            config: Arc::new(spu_config),
            leaders_state: ReplicaLeadersState::new_shared(),
            followers_state: FollowersState::new_shared(),
            spu_followers: FollowerNotifier::shared(),
            status_update: StatusMessageSink::shared(),
            sm_engine: SmartEngine::new(),
            leaders: LeaderConnections::shared(spus, replicas),
            metrics,
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

    pub fn smartmodule_localstore(&self) -> &SmartModuleLocalStore {
        &self.smartmodule_localstore
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
    #[instrument(skip(self))]
    pub async fn sync_follower_update(&self) {
        self.spu_followers
            .sync_from_spus(self.spu_localstore(), self.local_spu_id())
            .await;
    }

    pub fn smartengine_owned(&self) -> SmartEngine {
        self.sm_engine.clone()
    }

    #[allow(unused)]
    pub fn leaders(&self) -> Arc<LeaderConnections> {
        self.leaders.clone()
    }

    pub(crate) fn metrics(&self) -> Arc<SpuMetrics> {
        self.metrics.clone()
    }
}

mod file_replica {

    use fluvio_controlplane::{ReplicaRemovedRequest, UpdateReplicaRequest};
    use fluvio_storage::{FileReplica};
    use flv_util::actions::Actions;
    use tracing::{trace, warn};

    use crate::core::SpecChange;

    use super::*;

    #[derive(Debug)]
    pub enum ReplicaChange {
        Remove(ReplicaRemovedRequest),
        StorageError(anyhow::Error),
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

        pub async fn apply_replica_update(
            &self,
            request: UpdateReplicaRequest,
        ) -> Vec<ReplicaChange> {
            let changes = self
                .replica_localstore()
                .apply(request.all, request.changes);

            self.apply_replica_actions(changes).await
        }

        /// apply changes to
        #[instrument(
            skip(self),
            fields(actions=actions.count()))]
        async fn apply_replica_actions(
            &self,
            actions: Actions<SpecChange<Replica>>,
        ) -> Vec<ReplicaChange> {
            trace!( actions = ?actions,"replica actions");

            if actions.count() == 0 {
                debug!("no replica actions to process. ignoring");
                return vec![];
            }

            let local_id = self.local_spu_id();

            let mut outputs = vec![];
            for replica_action in actions.into_iter() {
                debug!(action = ?replica_action,"applying");

                match replica_action {
                    SpecChange::Add(new_replica) => {
                        if new_replica.is_being_deleted {
                            outputs.push(ReplicaChange::Remove(
                                self.remove_leader_replica(new_replica).await,
                            ));
                        } else if new_replica.leader == local_id {
                            // we are leader
                            if let Err(err) = self
                                .leaders_state()
                                .add_leader_replica(self, new_replica, self.status_update.clone())
                                .await
                            {
                                outputs.push(ReplicaChange::StorageError(err));
                            }
                        } else {
                            // add follower if we are in follower list
                            if new_replica.replicas.contains(&local_id) {
                                if let Err(err) = self
                                    .followers_state_owned()
                                    .add_replica(self, new_replica)
                                    .await
                                {
                                    outputs.push(ReplicaChange::StorageError(err));
                                }
                            } else {
                                debug!(replica = %new_replica.id, "not application to this spu, ignoring");
                            }
                        }
                    }
                    SpecChange::Delete(deleted_replica) => {
                        if deleted_replica.leader == local_id {
                            outputs.push(ReplicaChange::Remove(
                                self.remove_leader_replica(deleted_replica).await,
                            ));
                        } else {
                            self.remove_follower_replica(deleted_replica).await;
                        }
                    }
                    SpecChange::Mod(new_replica, old_replica) => {
                        if new_replica.is_being_deleted {
                            if new_replica.leader == local_id {
                                outputs.push(ReplicaChange::Remove(
                                    self.remove_leader_replica(new_replica).await,
                                ));
                            } else {
                                self.remove_follower_replica(new_replica).await
                            }
                        } else {
                            // check for leader change
                            if new_replica.leader != old_replica.leader {
                                if new_replica.leader == local_id {
                                    self.promote(&new_replica, &old_replica).await
                                } else {
                                    // we are follower
                                    // if we were leader before, we demote out self
                                    if old_replica.leader == local_id {
                                        self.demote_replica(new_replica).await
                                    } else {
                                        // we stay as follower but we switch to new leader
                                        debug!(
                                            "still follower but switching leader: {}",
                                            new_replica
                                        );
                                        self.switch_leader_for_follower(new_replica, old_replica)
                                            .await
                                    }
                                }
                            } else if new_replica.leader == local_id {
                                if self.leaders_state().get(&new_replica.id).await.is_some() {
                                } else {
                                    error!("leader controller was not found: {}", new_replica.id);
                                }
                            } else {
                                self.followers_state().update_replica(new_replica).await;
                            }
                        }
                    }
                }
            }

            outputs
        }

        /// reemove leader replica
        #[instrument(
            skip(self,replica),
            fields(
                replica = %replica.id,
            )
        )]
        async fn remove_leader_replica(&self, replica: Replica) -> ReplicaRemovedRequest {
            // try to send message to leader controller if still exists
            if let Some(previous_state) = self.leaders_state().remove(&replica.id).await {
                if let Err(err) = previous_state.remove().await {
                    error!("error: {} removing replica: {}", err, replica);
                } else {
                    debug!(
                        replica = %replica.id,
                        "leader remove was removed"
                    );
                }
            } else {
                // if we don't find existing replica, just warning
                warn!("no existing replica found {}", replica);
            }

            ReplicaRemovedRequest::new(replica.id, true)
        }

        /// reemove leader replica
        #[instrument(
            skip(self,replica),
            fields(
                replica = %replica.id,
            )
        )]
        async fn remove_follower_replica(&self, replica: Replica) {
            debug!("removing follower replica: {}", replica);
            if let Some(replica_state) = self
                .followers_state()
                .remove_replica(replica.leader, &replica.id)
                .await
            {
                if let Err(err) = replica_state.remove().await {
                    error!("error {}, removing replica: {}", err, replica);
                }
            } else {
                error!("there was no follower replica: {} to remove", replica);
            }
        }

        /// Demote leader replica as follower.
        /// This only happens on manual election
        #[instrument(
            skip(self,replica),
            fields(
                replica = %replica.id,
            )
        )]
        pub async fn demote_replica(&self, replica: Replica) {
            if let Some(leader_replica_state) = self.leaders_state().remove(&replica.id).await {
                drop(leader_replica_state);
                if let Err(err) = self
                    .followers_state_owned()
                    .add_replica(self, replica)
                    .await
                {
                    error!("demotion failed: {}", err);
                }
            } else {
                error!("leader controller was not found: {}", replica.id)
            }
        }

        /// Demote leader replica as follower.
        /// This only happens on manual election
        #[instrument(
            skip(self,new,old),
            fields(
                new = %new.id,
                old = %old.id,
            )
        )]
        async fn switch_leader_for_follower(&self, new: Replica, old: Replica) {
            // we stay as follower but we switch to new leader
            debug!("still follower but switching leader: {}", new);
            if self
                .followers_state()
                .remove_replica(old.leader, &old.id)
                .await
                .is_none()
            {
                error!("there was no follower replica: {} to switch", new);
            }
            if let Err(err) = self.followers_state_owned().add_replica(self, new).await {
                error!("leader switch failed: {}", err);
            }
        }
    }
}
