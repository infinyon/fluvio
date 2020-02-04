use std::sync::Arc;

use chashmap::CHashMap;
use chashmap::ReadGuard;
use chashmap::WriteGuard;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::SendError;
use futures::SinkExt;
use log::debug;
use log::warn;
use log::trace;
use log::error;

use flv_metadata::partition::ReplicaKey;
use kf_protocol::api::DefaultRecords;
use flv_storage::FileReplica;
use kf_protocol::fs::FilePartitionResponse;
use kf_protocol::api::Offset;
use kf_protocol::api::Isolation;
use kf_protocol::api::ErrorCode;
use flv_util::SimpleConcurrentBTreeMap;

use crate::InternalServerError;

use super::LeaderReplicaState;
use super::LeaderReplicaControllerCommand;

pub type SharedReplicaLeadersState<S> = Arc<ReplicaLeadersState<S>>;

/// State for Replica Leaders
/// It is used by Replica Leader Controller supervisor
#[derive(Debug)]
pub struct ReplicaLeadersState<S> {
    replicas: CHashMap<ReplicaKey, LeaderReplicaState<S>>,
    mailboxes: SimpleConcurrentBTreeMap<ReplicaKey, Sender<LeaderReplicaControllerCommand>>,
}

impl<S> Default for ReplicaLeadersState<S> {
    fn default() -> Self {
        ReplicaLeadersState {
            replicas: CHashMap::default(),
            mailboxes: SimpleConcurrentBTreeMap::new(),
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

    pub fn insert_replica(
        &self,
        key: ReplicaKey,
        leader: LeaderReplicaState<S>,
        mailbox: Sender<LeaderReplicaControllerCommand>,
    ) -> Option<LeaderReplicaState<S>> {
        trace!("adding insert leader state: {}", key);
        if let Some(mut old_mailbox) = self.mailboxes.write().insert(key.clone(), mailbox) {
            error!("closing left over mailbox for leader");
            old_mailbox.close_channel();
        }
        self.replicas.insert(key, leader)
    }

    /// remove leader replica
    /// we also remove mailbox and close it's channel which will terminated the controller
    pub fn remove_replica(&self, key: &ReplicaKey) -> Option<LeaderReplicaState<S>> {
        if let Some(replica) = self.replicas.remove(key) {
            if let Some(mut mailbox) = self.mailboxes.write().remove(key) {
                debug!("closing old leader mailbox: {}", key);
                mailbox.close_channel();
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
    ) -> Result<bool, SendError> {
        match self.mailbox(replica) {
            Some(mut mailbox) => {
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

    #[allow(dead_code)]
    pub fn mailbox(&self, key: &ReplicaKey) -> Option<Sender<LeaderReplicaControllerCommand>> {
        self.mailboxes
            .read()
            .get(key)
            .map(|mailbox_guard| mailbox_guard.clone())
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
        isolation: Isolation,
        response: &mut FilePartitionResponse,
    ) -> Option<(Offset,Offset)> {

        if let Some(leader_replica) = self.get_replica(rep_id) {
            Some(leader_replica
                .read_records(offset, isolation, response)
                .await)
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
        records: DefaultRecords,
        update_hw: bool,
    ) -> Result<bool, InternalServerError> {
        if let Some(mut leader_replica) = self.get_mut_replica(rep_id) {
            leader_replica
                .send_records(records, update_hw)
                .await?;
            self.send_message(rep_id, LeaderReplicaControllerCommand::EndOffsetUpdated)
                .await
                .map_err(|err| err.into())
        } else {
            warn!("no replica is found: {}", rep_id);
            Ok(false)
        }
    }
}

/*
/// encapsulate LeaderReplicaState that has readable lock
struct ReadableLeaderReplicaState<'a,S>(ReadGuard<'a,ReplicaKey, LeaderReplicaState<S>>);

impl <'a,S>ReadableLeaderReplicaState<'a,S>  {



}


impl <'a,S>From<ReadGuard<'a,ReplicaKey, LeaderReplicaState<S>>> for ReadableLeaderReplicaState<'a,S> {
    fn from(replica_state: ReadGuard<'a,ReplicaKey, LeaderReplicaState<S>>) -> Self {
        Self(replica_state)
    }
}
*/

#[cfg(test)]
mod test_channel {

    use std::time::Duration;

    use futures::channel::mpsc::Sender;
    use futures::channel::mpsc::Receiver;
    use futures::channel::mpsc::channel;
    use futures::future::join;
    use futures::SinkExt;
    use futures::StreamExt;

    use flv_future_core::sleep;
    use flv_future_core::test_async;

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
