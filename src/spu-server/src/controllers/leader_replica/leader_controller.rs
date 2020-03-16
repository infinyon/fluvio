use std::sync::Arc;
use std::time::Duration;

use log::debug;
use log::trace;
use log::warn;
use futures::channel::mpsc::Receiver;
use futures::future::FutureExt;
use futures::future::join3;
use futures::future::join;
use futures::select;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use flv_future_aio::timer::sleep;
use flv_metadata::partition::ReplicaKey;
use flv_storage::FileReplica;
use types::SpuId;
use kf_socket::ExclusiveKfSink;
use flv_future_aio::sync::mpsc::Sender;

use crate::core::SharedSpuSinks;
use crate::core::OffsetUpdateEvent;

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
    offset_sender: Sender<OffsetUpdateEvent>
}

impl<S> ReplicaLeaderController<S> {

    pub fn new(
        local_spu: SpuId,
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        leaders_state: SharedReplicaLeadersState<S>,
        follower_sinks: SharedSpuSinks,
        sc_sink: Arc<ExclusiveKfSink>,
        offset_sender: Sender<OffsetUpdateEvent>
    ) -> Self {
        Self {
            local_spu,
            id,
            controller_receiver,
            leaders_state,
            follower_sinks,
            sc_sink,
            offset_sender
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
                                join3(self.send_status_to_sc(),self.sync_followers(),self.update_offset_to_clients()).await;
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

    /// update the clients that we have offset changed
    async fn update_offset_to_clients(&self) {

        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            let event = OffsetUpdateEvent {
                hw: leader_replica.hw(),
                leo: leader_replica.leo(),
                replica_id: self.id.clone()
            };

            self.offset_sender.send(event).await;
            
        } else {
            warn!("no replica is found: {} for offset update", self.id);
        }

    }
}
