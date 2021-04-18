use dataplane::Isolation;
use tracing::{debug};
use tracing::instrument;
use async_channel::Receiver;
use futures_util::stream::StreamExt;

use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_storage::FileReplica;

use crate::control_plane::SharedSinkMessageChannel;

use super::LeaderReplicaControllerCommand;
use super::replica_state::{SharedLeaderState};

/// Controller for managing leader replica.
/// Each leader replica controller is spawned and managed by master controller to ensure max parallism.
pub struct ReplicaLeaderController<S> {
    id: ReplicaKey,
    controller_receiver: Receiver<LeaderReplicaControllerCommand>,
    state: SharedLeaderState<S>,
    sc_channel: SharedSinkMessageChannel,
}

impl<S> ReplicaLeaderController<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ReplicaKey,
        controller_receiver: Receiver<LeaderReplicaControllerCommand>,
        state: SharedLeaderState<S>,
        sc_channel: SharedSinkMessageChannel,
    ) -> Self {
        Self {
            id,
            controller_receiver,
            state,
            sc_channel,
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
            debug!("waiting for next command");

            select! {


                offset = hw_listener.listen() => {
                    debug!(hw_update = offset);
                    self.send_status_to_sc().await;
                },

                offset = leo_listener.listen() => {
                    debug!(leo_update = offset);
                    self.send_status_to_sc().await;
                },

                controller_req = self.controller_receiver.next() => {
                    if let Some(command) = controller_req {
                        match command {

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

    /// send status back to sc
    #[instrument(skip(self))]
    async fn send_status_to_sc(&self) {
        self.state.send_status_to_sc(&self.sc_channel).await;
    }
}
