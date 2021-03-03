

use tracing::{debug};
use tracing::instrument;
use async_channel::Receiver;
use futures_util::future::join3;
use futures_util::future::join;
use futures_util::stream::StreamExt;

use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_storage::FileReplica;
use fluvio_types::SpuId;
use tokio::sync::broadcast::Sender;

use crate::core::SharedSpuSinks;
use crate::core::OffsetUpdateEvent;
use crate::controllers::sc::SharedSinkMessageChannel;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::replica_state::{ LeaderReplicaState,SharedLeaderState};

/// time for complete re-sync with followers
pub const FOLLOWER_RECONCILIATION_INTERVAL_SEC: u64 = 300; // 5 min

/// Controller for managing leader replica.
/// Each leader replica controller is spawned and managed by master controller to ensure max parallism.
pub struct ReplicaLeaderController<S> {
    #[allow(dead_code)]
    local_spu: SpuId,
    id: ReplicaKey,
    controller_receiver: Receiver<LeaderReplicaControllerCommand>,
    state: SharedLeaderState<S>,
    follower_sinks: SharedSpuSinks,
    sc_channel: SharedSinkMessageChannel,
    offset_sender: Sender<OffsetUpdateEvent>,
    max_bytes: u32
}

impl<S> ReplicaLeaderController<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_spu: SpuId,
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        state: SharedLeaderState<S>,
        follower_sinks: SharedSpuSinks,
        sc_channel: SharedSinkMessageChannel,
        offset_sender: Sender<OffsetUpdateEvent>,
        max_bytes: u32,
    ) -> Self {
        Self {
            local_spu,
            id,
            controller_receiver,
            state,
            follower_sinks,
            sc_channel,
            offset_sender,
            max_bytes,
        }
    }
}


impl ReplicaLeaderController<FileReplica> {
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    #[instrument(
        skip(self),
        fields(replica_id = %self.id),
        name = "LeaderController",
    )]
    async fn dispatch_loop(mut self) {
        use tokio::select;


        self.send_status_to_sc().await;

        loop {
            self.sync_followers().await;
            debug!("waiting for next command");

            select! {

                controller_req = self.controller_receiver.next() => {
                    if let Some(command) = controller_req {
                        match command {
                            LeaderReplicaControllerCommand::EndOffsetUpdated => {
                                debug!("leader replica end offset has updated, update the follower if need to be");
                                join3(self.send_status_to_sc(),self.sync_followers(),self.update_offset_to_clients()).await;
                            },

                            LeaderReplicaControllerCommand::FollowerOffsetUpdate(offsets) => {
                                self.update_follower_offsets(offsets).await;
                            },

                            LeaderReplicaControllerCommand::UpdateReplicaFromSc(_) => {
                                debug!("update replica from sc");
                            },
                            LeaderReplicaControllerCommand::RemoveReplicaFromSc => {
                                debug!("RemoveReplica command, exiting");
                                break;
                            }
                        }
                    } else {
                        debug!(
                            "mailbox has terminated, terminating loop"
                        );
                    }
                }
            }
        }

        debug!("terminated");
    }

    /// update the follower offsets
    async fn update_follower_offsets(&mut self, offsets: FollowerOffsetUpdate) {
       
        let follower_id = offsets.follower_id;
        let (update_status, sync_follower) = self.state.update_follower_offsets(offsets).await;
        join(
            async {
                if update_status {
                    self.state.send_status_to_sc(&self.sc_channel).await;
                }
            },
            async {
                if let Some(follower_info) = sync_follower {
                    self.state
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
        
    }

    /// go thru each of follower and sync replicas
    #[instrument(skip(self))]
    async fn sync_followers(&self) {
        self.state.sync_followers(&self.follower_sinks, self.max_bytes)
                .await;
    }

    /// send status back to sc
    #[instrument(skip(self))]
    async fn send_status_to_sc(&self) {
        self.state.send_status_to_sc(&self.sc_channel).await;
    }

    /// update the clients that we have offset changed
    async fn update_offset_to_clients(&self) {
        /* 
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
        */
    }
}
