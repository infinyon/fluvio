use dataplane::Isolation;
use tracing::{debug, error};
use tracing::instrument;
use async_channel::Receiver;
use futures_util::future::{join, join3};
use futures_util::stream::StreamExt;

use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_storage::FileReplica;

use crate::core::SharedSpuSinks;
use crate::control_plane::SharedSinkMessageChannel;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::replica_state::{SharedLeaderState};

/// time for complete re-sync with followers
//pub const FOLLOWER_RECONCILIATION_INTERVAL_SEC: u64 = 300; // 5 min

/// Controller for managing leader replica.
/// Each leader replica controller is spawned and managed by master controller to ensure max parallism.
pub struct ReplicaLeaderController<S> {
    id: ReplicaKey,
    controller_receiver: Receiver<LeaderReplicaControllerCommand>,
    state: SharedLeaderState<S>,
    follower_sinks: SharedSpuSinks,
    sc_channel: SharedSinkMessageChannel,
    max_bytes: u32,
}

impl<S> ReplicaLeaderController<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        state: SharedLeaderState<S>,
        follower_sinks: SharedSpuSinks,
        sc_channel: SharedSinkMessageChannel,
        max_bytes: u32,
    ) -> Self {
        Self {
            id,
            controller_receiver,
            state,
            follower_sinks,
            sc_channel,
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

        let mut hw_listener = self.state.offset_listener(&Isolation::ReadCommitted);
        let mut leo_listener = self.state.offset_listener(&Isolation::ReadUncommitted);
        loop {
            self.sync_followers().await;
            debug!("waiting for next command");

            select! {


                offset = hw_listener.listen() => {
                    debug!(hw_update = offset);
                    join(self.send_status_to_sc(),self.sync_followers()).await;
                },

                offset = leo_listener.listen() => {
                    debug!(leo_update = offset);
                    join(self.send_status_to_sc(),self.sync_followers()).await;
                },

                controller_req = self.controller_receiver.next() => {
                    if let Some(command) = controller_req {
                        match command {

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
                        break;
                    }
                }
            }
        }

        debug!("terminated");
    }

    /// update the follower offsets
    async fn update_follower_offsets(&mut self, offsets: FollowerOffsetUpdate) {
        let follower_id = offsets.follower_id;
        let (update_status, sync_follower, hw_update) = self.state.update_followers(offsets).await;
        join3(
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
            async {
                if let Some(hw) = hw_update {
                    if let Err(err) = self.state.update_hw(hw).await {
                        error!("error updating hw: {}", err);
                    };
                }
            },
        )
        .await;
    }

    /// go thru each of follower and sync replicas
    #[instrument(skip(self))]
    async fn sync_followers(&self) {
        self.state
            .sync_followers(&self.follower_sinks, self.max_bytes)
            .await;
    }

    /// send status back to sc
    #[instrument(skip(self))]
    async fn send_status_to_sc(&self) {
        self.state.send_status_to_sc(&self.sc_channel).await;
    }
}
