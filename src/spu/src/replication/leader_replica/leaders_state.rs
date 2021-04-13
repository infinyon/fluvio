use std::{
    ops::{Deref, DerefMut},
};

use tracing::{debug, error};
use tracing::instrument;
use dashmap::DashMap;
use async_channel::Receiver;

use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use fluvio_storage::{FileReplica, StorageError};

use crate::{control_plane::SharedSinkMessageChannel, core::SharedGlobalContext};

use super::{
    LeaderReplicaControllerCommand, LeaderReplicaState, ReplicaLeaderController,
    replica_state::SharedLeaderState,
};

pub type SharedReplicaLeadersState<S> = ReplicaLeadersState<S>;

/// Collection of replicas
#[derive(Debug)]
pub struct ReplicaLeadersState<S>(DashMap<ReplicaKey, SharedLeaderState<S>>);

impl<S> Default for ReplicaLeadersState<S> {
    fn default() -> Self {
        ReplicaLeadersState(DashMap::new())
    }
}

impl<S> Deref for ReplicaLeadersState<S> {
    type Target = DashMap<ReplicaKey, SharedLeaderState<S>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for ReplicaLeadersState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S> ReplicaLeadersState<S> {
    pub fn new_shared() -> SharedReplicaLeadersState<S> {
        Self::default()
    }
}

impl ReplicaLeadersState<FileReplica> {
    #[instrument(
        skip(self, ctx,replica,sink_channel),
        fields(replica = %replica.id)
    )]
    pub async fn add_leader_replica(
        &self,
        ctx: SharedGlobalContext<FileReplica>,
        replica: Replica,
        max_bytes: u32,
        sink_channel: SharedSinkMessageChannel,
    ) -> Result<LeaderReplicaState<FileReplica>, StorageError> {
        let replica_id = replica.id.clone();

        match LeaderReplicaState::create(replica, ctx.config()).await {
            Ok((leader_replica, receiver)) => {
                debug!("file replica created and spawing leader controller");
                self.spawn_leader_controller(
                    ctx.clone(),
                    replica_id,
                    leader_replica.clone(),
                    receiver,
                    max_bytes,
                    sink_channel,
                )
                .await;

                Ok(leader_replica)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument(
        skip(self,ctx, replica_id, leader_state,sink_channel),
        fields(replica = %replica_id)
    )]
    pub async fn spawn_leader_controller(
        &self,
        ctx: SharedGlobalContext<FileReplica>,
        replica_id: ReplicaKey,
        leader_state: LeaderReplicaState<FileReplica>,
        receiver: Receiver<LeaderReplicaControllerCommand>,
        max_bytes: u32,
        sink_channel: SharedSinkMessageChannel,
    ) {
        if let Some(old_replica) = self.insert(replica_id.clone(), leader_state.clone()) {
            error!(
                "there was existing replica when creating new leader replica: {}",
                old_replica.id()
            );
        }

        let leader_controller = ReplicaLeaderController::new(
            replica_id,
            receiver,
            leader_state,
            ctx.followers_sink_owned(),
            sink_channel,
            max_bytes,
        );
        leader_controller.run();
    }
}

#[cfg(test)]
mod test_channel {

    use std::time::Duration;

    use async_channel::Sender;
    use async_channel::Receiver;
    use async_channel::bounded as channel;
    use futures_util::future::join;
    use futures_util::StreamExt;

    use fluvio_future::timer::sleep;
    use fluvio_future::test_async;

    async fn receiver_tst(mut receiver: Receiver<u16>) {
        // sleep to let sender send messages
        assert!(receiver.next().await.is_some());
        // wait until sender send all 3 and terminate sender
        sleep(Duration::from_millis(10)).await;
        assert!(receiver.next().await.is_some());
        assert!(receiver.next().await.is_some());
        assert!(receiver.next().await.is_none());
    }

    async fn sender_test(orig_mailbox: Sender<u16>) {
        let mailbox = orig_mailbox.clone();
        assert!(!mailbox.is_closed());
        sleep(Duration::from_millis(1)).await;
        mailbox.send(10).await.expect("send");
        mailbox.send(11).await.expect("send");
        mailbox.send(12).await.expect("send");
        mailbox.close();
        assert!(mailbox.is_closed());
        // wait 30 millisecond to allow test of receiver
        sleep(Duration::from_millis(30)).await;
    }

    // test send and disconnect
    #[test_async]
    async fn test_event_shutdown() -> Result<(), ()> {
        let (sender, receiver) = channel::<u16>(10);

        join(sender_test(sender), receiver_tst(receiver)).await;

        Ok(())
    }
}
