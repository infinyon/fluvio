use std::{
    ops::{Deref},
};
use std::sync::RwLock;
use std::collections::HashMap;

use tracing::{debug, error, instrument};

use fluvio_controlplane_metadata::partition::{Replica, ReplicaKey};
use fluvio_storage::{FileReplica, StorageError};

use crate::{control_plane::SharedStatusUpdate, core::SharedGlobalContext};

use super::{LeaderReplicaState, replica_state::SharedLeaderState};

pub type SharedReplicaLeadersState<S> = ReplicaLeadersState<S>;

/// Collection of replicas
#[derive(Debug)]
pub struct ReplicaLeadersState<S>(RwLock<HashMap<ReplicaKey, SharedLeaderState<S>>>);

impl<S> Default for ReplicaLeadersState<S> {
    fn default() -> Self {
        ReplicaLeadersState(RwLock::new(HashMap::new()))
    }
}

impl<S> Deref for ReplicaLeadersState<S> {
    type Target = RwLock<HashMap<ReplicaKey, SharedLeaderState<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> ReplicaLeadersState<S> {
    pub fn new_shared() -> SharedReplicaLeadersState<S> {
        Self::default()
    }
}

impl<S> ReplicaLeadersState<S> {
    /// get clone of state
    pub fn get(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let read = self.read().unwrap();
        read.get(replica).map(|value| value.clone())
    }

    pub fn remove(&self, replica: &ReplicaKey) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().unwrap();
        writer.remove(replica)
    }

    #[allow(unused)]
    pub fn insert(
        &self,
        replica: ReplicaKey,
        state: SharedLeaderState<S>,
    ) -> Option<SharedLeaderState<S>> {
        let mut writer = self.write().unwrap();
        writer.insert(replica, state)
    }
}

impl ReplicaLeadersState<FileReplica> {
    #[instrument(
        skip(self, ctx,replica,status_update),
        fields(replica = %replica.id)
    )]
    pub async fn add_leader_replica(
        &self,
        ctx: SharedGlobalContext<FileReplica>,
        replica: Replica,
        max_bytes: u32,
        status_update: SharedStatusUpdate,
    ) -> Result<LeaderReplicaState<FileReplica>, StorageError> {
        let replica_id = replica.id.clone();

        let leader_replica =
            LeaderReplicaState::create(replica, ctx.config(), status_update).await?;
        self.insert_leader(replica_id, leader_replica.clone()).await;
        Ok(leader_replica)
    }

    #[instrument(
        skip(self,replica_id, leader_state),
        fields(replica = %replica_id)
    )]
    async fn insert_leader(
        &self,
        replica_id: ReplicaKey,
        leader_state: LeaderReplicaState<FileReplica>,
    ) {
        let mut writer = self.write().unwrap();
        if let Some(old_replica) = writer.insert(replica_id.clone(), leader_state.clone()) {
            error!(
                "there was existing replica when creating new leader replica: {}",
                old_replica.id()
            );
        }
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
