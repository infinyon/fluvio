use std::sync::Arc;
use std::collections::HashMap;

use chashmap::CHashMap;
use chashmap::ReadGuard;
use chashmap::WriteGuard;
use async_channel::Sender;
use async_channel::SendError;
use async_rwlock::RwLock;
use tracing::debug;
use tracing::warn;
use tracing::trace;
use tracing::error;

use fluvio_controlplane_metadata::partition::ReplicaKey;
use dataplane::record::RecordSet;
use fluvio_storage::FileReplica;
use dataplane::fetch::FilePartitionResponse;
use dataplane::{Offset, Isolation, ErrorCode};

use crate::InternalServerError;

use super::LeaderReplicaState;
use super::LeaderReplicaControllerCommand;

pub type SharedReplicaLeadersState<S> = Arc<ReplicaLeadersState<S>>;

/// State for Replica Leaders
/// It is used by Replica Leader Controller supervisor
#[derive(Debug)]
pub struct ReplicaLeadersState<S> {
    replicas: CHashMap<ReplicaKey, LeaderReplicaState<S>>,
    mailboxes: RwLock<HashMap<ReplicaKey, Sender<LeaderReplicaControllerCommand>>>,
}

impl<S> Default for ReplicaLeadersState<S> {
    fn default() -> Self {
        ReplicaLeadersState {
            replicas: CHashMap::default(),
            mailboxes: RwLock::new(HashMap::new()),
        }
    }
}

impl<S> ReplicaLeadersState<S> {
    pub fn new_shared() -> Arc<ReplicaLeadersState<S>> {
        Arc::new(Self::default())
    }

    pub fn has_replica(&self, key: &ReplicaKey) -> bool {
        self.replicas.contains_key(key)
    }

    pub fn get_replica(
        &self,
        key: &ReplicaKey,
    ) -> Option<ReadGuard<ReplicaKey, LeaderReplicaState<S>>> {
        self.replicas.get(key)
    }

    pub fn get_mut_replica(
        &self,
        key: &ReplicaKey,
    ) -> Option<WriteGuard<ReplicaKey, LeaderReplicaState<S>>> {
        self.replicas.get_mut(key)
    }

    pub async fn insert_replica(
        &self,
        key: ReplicaKey,
        leader: LeaderReplicaState<S>,
        mailbox: Sender<LeaderReplicaControllerCommand>,
    ) -> Option<LeaderReplicaState<S>> {
        trace!("adding insert leader state: {}", key);
        if let Some(old_mailbox) = self.mailboxes.write().await.insert(key.clone(), mailbox) {
            error!("closing left over mailbox for leader");
            old_mailbox.close();
        }
        self.replicas.insert(key, leader)
    }

    /// remove leader replica
    /// we also remove mailbox and close it's channel which will terminated the controller
    pub async fn remove_replica(&self, key: &ReplicaKey) -> Option<LeaderReplicaState<S>> {
        if let Some(replica) = self.replicas.remove(key) {
            if let Some(mailbox) = self.mailboxes.write().await.remove(key) {
                debug!("closing old leader mailbox: {}", key);
                mailbox.close();
            } else {
                error!("no mailbox found for removing: {}", key);
            }
            Some(replica)
        } else {
            error!("leader replica: {} is not founded", key);
            None
        }
    }

    /// send message to leader controller
    pub async fn send_message(
        &self,
        replica: &ReplicaKey,
        command: LeaderReplicaControllerCommand,
    ) -> Result<bool, SendError<LeaderReplicaControllerCommand>> {
        match self.mailbox(replica).await {
            Some(mailbox) => {
                trace!(
                    "sending message to leader replica: {:#?} controller: {:#?}",
                    replica,
                    command
                );
                mailbox.send(command).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub async fn mailbox(
        &self,
        key: &ReplicaKey,
    ) -> Option<Sender<LeaderReplicaControllerCommand>> {
        self.mailboxes.read().await.get(key).cloned()
    }
}

impl ReplicaLeadersState<FileReplica> {
    /// write records to response
    ///
    /// # Arguments
    ///
    /// * `ctx` - Shared Context, this is thread safe
    /// * `rep_id` - replication id
    /// * `offset` - starting offset
    /// * `isolation` - isolation
    /// * `partition_response` - response
    /// return associated hw, leo
    pub async fn read_records(
        &self,
        rep_id: &ReplicaKey,
        offset: Offset,
        max_len: u32,
        isolation: Isolation,
        response: &mut FilePartitionResponse,
    ) -> Option<(Offset, Offset)> {
        if let Some(leader_replica) = self.get_replica(rep_id) {
            Some(
                leader_replica
                    .read_records(offset, max_len, isolation, response)
                    .await,
            )
        } else {
            warn!("no replica is found: {}", rep_id);
            response.error_code = ErrorCode::NotLeaderForPartition;
            None
        }
    }

    /// write new record and notify the leader replica controller
    /// TODO: may replica should be moved it's own map
    pub async fn send_records(
        &self,
        rep_id: &ReplicaKey,
        records: RecordSet,
        update_hw: bool,
    ) -> Result<bool, InternalServerError> {
        if let Some(mut leader_replica) = self.get_mut_replica(rep_id) {
            leader_replica.send_records(records, update_hw).await?;
            self.send_message(rep_id, LeaderReplicaControllerCommand::EndOffsetUpdated)
                .await
                .map_err(|err| err.into())
        } else {
            warn!("no replica is found: {}", rep_id);
            Ok(false)
        }
    }
}

#[cfg(test)]
mod test_channel {

    use std::time::Duration;

    use futures::channel::mpsc::Sender;
    use futures::channel::mpsc::Receiver;
    use futures::channel::mpsc::channel;
    use futures::future::join;
    use futures::SinkExt;
    use futures::StreamExt;

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
        let mut mailbox = orig_mailbox.clone();
        assert!(!mailbox.is_closed());
        sleep(Duration::from_millis(1)).await;
        mailbox.send(10).await.expect("send");
        mailbox.send(11).await.expect("send");
        mailbox.send(12).await.expect("send");
        mailbox.disconnect();
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
