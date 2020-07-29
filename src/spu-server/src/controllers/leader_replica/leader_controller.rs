use std::sync::Arc;
use std::time::Duration;

use log::debug;
use log::warn;
use log::error;
use futures::channel::mpsc::Receiver;
use futures::future::FutureExt;
use futures::future::join3;
use futures::future::join;
use futures::select;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use flv_future_aio::timer::sleep;
use flv_metadata_cluster::partition::ReplicaKey;
use flv_storage::FileReplica;
use flv_types::SpuId;
use kf_socket::ExclusiveKfSink;
use flv_future_aio::sync::broadcast::Sender;

use crate::core::SharedSpuSinks;
use crate::core::OffsetUpdateEvent;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::SharedReplicaLeadersState;

/// time for complete re-sync with followers
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
    offset_sender: Sender<OffsetUpdateEvent>,
    max_bytes: u32,
}

impl<S> ReplicaLeaderController<S> {
    pub fn new(
        local_spu: SpuId,
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        leaders_state: SharedReplicaLeadersState<S>,
        follower_sinks: SharedSpuSinks,
        sc_sink: Arc<ExclusiveKfSink>,
        offset_sender: Sender<OffsetUpdateEvent>,
        max_bytes: u32,
    ) -> Self {
        Self {
            local_spu,
            id,
            controller_receiver,
            leaders_state,
            follower_sinks,
            sc_sink,
            offset_sender,
            max_bytes,
        }
    }
}

/// debug leader, this should be only used in replica leader controller
/// example:  leader_debug!("{}",expression)
macro_rules! leader_debug {
    ($self:ident, $message:expr,$($arg:expr)*) => ( debug!(concat!("replica: <{}> => ",$message),$self.id, $($arg)*) ) ;

    ($self:ident, $message:expr) => ( debug!(concat!("replica: <{}> => ",$message),$self.id))
}

macro_rules! leader_warn {
    ($self:ident, $message:expr,$($arg:expr)*) => ( warn!(concat!("replica: <{}> => ",$message),$self.id, $($arg)*) ) ;

    ($self:ident, $message:expr) => ( warn!(concat!("replica: {} => ",$message),$self.id))
}

impl ReplicaLeaderController<FileReplica> {
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        leader_debug!(self, "starting");
        self.send_status_to_sc().await;
        self.sync_followers().await;
        loop {
            leader_debug!(self, "waiting for next command");

            select! {

                _ = (sleep(Duration::from_secs(FOLLOWER_RECONCILIATION_INTERVAL_SEC))).fuse() => {


                    self.sync_followers().await;
                },

                controller_req = self.controller_receiver.next() => {
                    if let Some(command) = controller_req {
                        match command {
                            LeaderReplicaControllerCommand::EndOffsetUpdated => {
                                leader_debug!(self,"leader replica end offset has updated, update the follower if need to be");
                                join3(self.send_status_to_sc(),self.sync_followers(),self.update_offset_to_clients()).await;
                            },

                            LeaderReplicaControllerCommand::FollowerOffsetUpdate(offsets) => {
                                leader_debug!(self,"Offset update from follower: {}", offsets);
                                self.update_follower_offsets(offsets).await;
                            },

                            LeaderReplicaControllerCommand::UpdateReplicaFromSc(replica) => {
                                leader_debug!(self,"update replica from sc: {}",replica.id);
                            }
                        }
                    } else {
                        leader_debug!(
                            self,
                            "mailbox has terminated, terminating loop"
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
                            .sync_follower(
                                &self.follower_sinks,
                                follower_id,
                                &follower_info,
                                self.max_bytes,
                            )
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

    /// go thru each of follower and sync replicas
    async fn sync_followers(&self) {
        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            leader_replica
                .sync_followers(&self.follower_sinks, self.max_bytes)
                .await;
        } else {
            leader_warn!(self, "sync followers: no replica is found");
        }
    }

    /// send status back to sc
    async fn send_status_to_sc(&self) {
        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            leader_replica.send_status_to_sc(&self.sc_sink).await;
        } else {
            leader_warn!(self, "no replica is found");
        }
    }

    /// update the clients that we have offset changed
    async fn update_offset_to_clients(&self) {
        if let Some(leader_replica) = self.leaders_state.get_replica(&self.id) {
            let event = OffsetUpdateEvent {
                hw: leader_replica.hw(),
                leo: leader_replica.leo(),
                replica_id: self.id.clone(),
            };

            if let Err(err) = self.offset_sender.send(event) {
                error!("error sending offset {:#?}", err);
            }
        } else {
            leader_warn!(self, "no replica is found");
        }
    }
}
