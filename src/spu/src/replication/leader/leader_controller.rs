use dataplane::Isolation;
use tracing::{debug};
use tracing::instrument;


use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_storage::FileReplica;

use crate::control_plane::SharedSinkMessageChannel;
use crate::storage::REMOVAL_START;

use super::replica_state::{SharedLeaderState};

/// Controller for managing leader replica.
/// Each leader replica controller is spawned and managed by master controller to ensure max parallism.
pub struct ReplicaLeaderController<S> {
    id: ReplicaKey,
    state: SharedLeaderState<S>,
    sc_channel: SharedSinkMessageChannel,
}

impl<S> ReplicaLeaderController<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ReplicaKey,
        state: SharedLeaderState<S>,
        sc_channel: SharedSinkMessageChannel,
    ) -> Self {
        Self {
            id,
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
    async fn dispatch_loop(self) {
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
                    if offset == REMOVAL_START {
                        debug!("replica is removed, shutting down");
                        break;
                    }
                    self.send_status_to_sc().await;
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
