//!
//! # Global Context
//!
//! Global Context maintains states need to be shared across in the SPU

use std::sync::Arc;
use std::fmt::Debug;

use tracing::{debug, error, instrument};

use fluvio_types::SpuId;
use fluvio_storage::ReplicaStorage;

use crate::config::SpuConfig;
use crate::control_plane::SharedMirrorStatusUpdate;
use crate::control_plane::StatusMirrorMessageSink;
use crate::kv::consumer::SharedConsumerOffsetStorages;
use crate::replication::follower::FollowersState;
use crate::replication::follower::SharedFollowersState;
use crate::replication::leader::{
    SharedReplicaLeadersState, ReplicaLeadersState, FollowerNotifier, SharedSpuUpdates,
};
use crate::control_plane::{StatusLrsMessageSink, SharedLrsStatusUpdate};
use crate::core::metrics::SpuMetrics;
use crate::smartengine::SmartEngine;

use super::leader_client::LeaderConnections;
use super::mirror::MirrorLocalStore;
use super::mirror::SharedMirrorLocalStore;
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
    lrs_status_update: SharedLrsStatusUpdate,
    mirror_status_update: SharedMirrorStatusUpdate,
    sm_engine: SmartEngine,
    leaders: Arc<LeaderConnections>,
    mirrors: SharedMirrorLocalStore,
    metrics: Arc<SpuMetrics>,
    consumer_offset: SharedConsumerOffsetStorages,
}

// -----------------------------------
// Global Context - Implementation
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
            lrs_status_update: StatusLrsMessageSink::shared(),
            mirror_status_update: StatusMirrorMessageSink::shared(),
            sm_engine: SmartEngine::new(),
            leaders: LeaderConnections::shared(spus, replicas),
            mirrors: MirrorLocalStore::new_shared(),
            metrics,
            consumer_offset: SharedConsumerOffsetStorages::default(),
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

    pub fn mirrors_localstore(&self) -> &MirrorLocalStore {
        &self.mirrors
    }

    pub fn mirrors_localstore_owned(&self) -> SharedMirrorLocalStore {
        self.mirrors.clone()
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

    pub fn follower_notifier(&self) -> &Arc<FollowerNotifier> {
        &self.spu_followers
    }

    pub fn follower_notifier_owned(&self) -> Arc<FollowerNotifier> {
        self.spu_followers.clone()
    }

    #[allow(unused)]
    pub fn status_update(&self) -> &StatusLrsMessageSink {
        &self.lrs_status_update
    }

    pub fn status_update_owned(&self) -> SharedLrsStatusUpdate {
        self.lrs_status_update.clone()
    }

    #[allow(unused)]
    pub fn mirror_status_update(&self) -> &StatusMirrorMessageSink {
        &self.mirror_status_update
    }

    pub fn mirror_status_update_owned(&self) -> SharedMirrorStatusUpdate {
        self.mirror_status_update.clone()
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

    pub(crate) fn consumer_offset(&self) -> &SharedConsumerOffsetStorages {
        &self.consumer_offset
    }
}

mod file_replica {

    use fluvio_controlplane::{
        sc_api::remove::ReplicaRemovedRequest, replica::Replica,
        spu_api::update_replica::UpdateReplicaRequest,
    };
    use fluvio_protocol::record::ReplicaKey;
    use fluvio_types::{defaults::CONSUMER_STORAGE_TOPIC, PartitionId};
    use tracing::{trace, warn};

    use fluvio_storage::FileReplica;
    use flv_util::actions::Actions;

    use crate::{core::SpecChange, kv::consumer::ConsumerOffsetKey};

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
        pub async fn promote(
            &self,
            new_replica: &Replica,
            old_replica: &Replica,
        ) -> Vec<ReplicaChange> {
            let mut outputs = Vec::new();
            if let Some(follower_replica) = self
                .followers_state()
                .remove_replica(old_replica.leader, &old_replica.id)
                .await
            {
                debug!(
                    replica = %old_replica.id,
                    "old follower replica exists, promoting to leader"
                );

                if let Err(err) = self
                    .leaders_state()
                    .promote_follower(
                        self.config().into(),
                        follower_replica,
                        new_replica.clone(),
                        self.status_update_owned(),
                        self,
                    )
                    .await
                {
                    error!("follower promotion failed: {err}");
                    outputs.push(ReplicaChange::StorageError(err));
                };
            } else {
                error!("follower replica {} didn't exists!", old_replica.id);
            }
            outputs
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
                            self.remove_replica(&mut outputs, new_replica).await;
                        } else if new_replica.leader == local_id {
                            // we are leader
                            if let Err(err) = self
                                .leaders_state()
                                .add_leader_replica(
                                    self,
                                    new_replica,
                                    self.lrs_status_update.clone(),
                                )
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
                        self.remove_replica(&mut outputs, deleted_replica).await;
                    }
                    SpecChange::Mod(new_replica, old_replica) => {
                        if new_replica.is_being_deleted {
                            self.remove_replica(&mut outputs, new_replica).await;
                        } else {
                            // check for leader change
                            if new_replica.leader != old_replica.leader {
                                if new_replica.leader == local_id {
                                    outputs
                                        .append(&mut self.promote(&new_replica, &old_replica).await)
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

        async fn remove_replica(&self, outputs: &mut Vec<ReplicaChange>, replica: Replica) {
            if let Err(err) = self.delete_consumers_offset(&replica).await {
                error!("error: {} deleting consumers offset: {}", err, replica);
            }

            if replica.leader == self.local_spu_id() {
                outputs.push(ReplicaChange::Remove(
                    self.remove_leader_replica(replica).await,
                ));
            } else {
                self.remove_follower_replica(replica).await;
            }
        }

        /// remove leader replica
        #[instrument(
            skip(self,replica),
            fields(
                replica = %replica.id,
            )
        )]
        async fn remove_leader_replica(&self, replica: Replica) -> ReplicaRemovedRequest {
            // try to send message to leader controller if still exists
            if let Some(previous_state) = self.leaders_state().remove(&replica.id).await {
                previous_state.signal_topic_deleted().await;

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

        /// remove leader replica
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

        /// Delete consumers offset for given replica if it is leader consumer
        async fn delete_consumers_offset(&self, replica: &Replica) -> anyhow::Result<()> {
            let consumers_replica_id =
                ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());
            let Some(ref replica_consumer) = self.leaders_state().get(&consumers_replica_id).await
            else {
                debug!("no consumer replica found");
                return Ok(());
            };

            let consumer_storage = self
                .consumer_offset()
                .get_or_insert(replica_consumer, self.follower_notifier())
                .await?;

            let consumer_offset_keys = consumer_storage
                .list()
                .await?
                .into_iter()
                .filter_map(|(key, _)| {
                    if key.replica_id == replica.id {
                        let key = ConsumerOffsetKey::new(key.replica_id, key.consumer_id);
                        Some(key)
                    } else {
                        None
                    }
                })
                .collect::<Vec<ConsumerOffsetKey>>();

            for key in consumer_offset_keys {
                consumer_storage.delete(&key).await?;
            }

            Ok(())
        }
    }
}
