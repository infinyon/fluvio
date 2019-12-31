use std::sync::Arc;
use std::time::Duration;

use log::debug;
use log::trace;
use log::warn;
use futures::channel::mpsc::Receiver;
use futures::future::FutureExt;
use futures::future::join;
use futures::select;
use futures::stream::StreamExt;

use flv_future_core::spawn;
use flv_future_core::sleep;
use flv_metadata::partition::ReplicaKey;
use flv_storage::FileReplica;
use types::SpuId;
use kf_socket::ExclusiveKfSink;

use crate::core::SharedSpuSinks;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::SharedReplicaLeadersState;

/// time for complete reconcillation with followers
pub const FOLLOWER_RECONCILIATION_INTERVAL_SEC: u64 = 300; // 5 min

/// Controller for managing leader replica.
/// Each leader replica controller is spawned and managed by master controller to ensure max parallism.
pub struct ReplicaLeaderController<S> {
    #[allow(dead_code)]
    local_spu: SpuId,
    id: ReplicaKey,
    controller_receiver: Receiver<LeaderReplicaControllerCommand>,
    leaders_state: SharedReplicaLeadersState<S>,
    follower_sinks: SharedSpuSinks,
    sc_sink: Arc<ExclusiveKfSink>,
}

impl<S> ReplicaLeaderController<S> {
    pub fn new(
        local_spu: SpuId,
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        leaders_state: SharedReplicaLeadersState<S>,
        follower_sinks: SharedSpuSinks,
        sc_sink: Arc<ExclusiveKfSink>,
    ) -> Self {
        Self {
            local_spu,
            id,
            controller_receiver,
            leaders_state,
            follower_sinks,
            sc_sink,
        }
    }
}

impl ReplicaLeaderController<FileReplica> {
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        debug!("starting leader controller for: {}", self.id);
        self.send_status_to_sc().await;
        self.sync_followers().await;
        loop {
            debug!("waiting for next command");

            select! {

                _ = (sleep(Duration::from_secs(FOLLOWER_RECONCILIATION_INTERVAL_SEC))).fuse() => {
                    debug!("timer fired - kickoff follower reconcillation");
                    self.sync_followers().await;
                },

                controller_req = self.controller_receiver.next() => {
                    if let Some(command) = controller_req {
                        match command {
                            LeaderReplicaControllerCommand::EndOffsetUpdated => {
                                trace!("leader replica endoffset has updated, update the follower if need to be");
                                join(self.send_status_to_sc(),self.sync_followers()).await;
                            },

                            LeaderReplicaControllerCommand::FollowerOffsetUpdate(offsets) => {
                                debug!("Offset update from follower: {:#?} for leader: {}", offsets, self.id);
                                self.update_follower_offsets(offsets).await;
                            },

                            LeaderReplicaControllerCommand::UpdateReplicaFromSc(replica) => {
                                debug!("update replica from sc: {}",replica.id);
                            }
                        }
                    } else {
                        debug!(
                            "mailbox has terminated for replica leader: {}, terminating loop",
                            self.id
                        );
                    }
                }
            }
        }
    }

    /// update the follower offsets
    async fn update_follower_offsets(&self, offsets: FollowerOffsetUpdate) {
        if let Some(mut leader_replica) = self.leaders_state.get_mut_replica(&self.id) {
            let follower_id = offsets.follower_id;
            let (update_status, sync_follower) = leader_replica.update_follower_offsets(offsets);
            join(
                async {
                    if update_status {
                        leader_replica.send_status_to_sc(&self.sc_sink).await;
                    }
                },
                async {
                    if let Some(follower_info) = sync_follower {
                        leader_replica
                            .sync_follower(&self.follower_sinks, follower_id, &follower_info)
                            .await;
                    }
                },
            )
            .await;
        } else {
            warn!(
                "no replica is found: {} for update follower offsets",
                self.id
            );
        }
    }

    /// update the follower with my state
    async fn sync_followers(&self) {
        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            leader_replica.sync_followers(&self.follower_sinks).await;
        } else {
            warn!("no replica is found: {} for sync followers", self.id);
        }
    }

    /// send status back to sc
    async fn send_status_to_sc(&self) {
        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            leader_replica.send_status_to_sc(&self.sc_sink).await;
        } else {
            warn!("no replica is found: {} for send status back", self.id);
        }
    }
}
