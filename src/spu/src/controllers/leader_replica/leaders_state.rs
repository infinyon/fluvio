use std::ops::{Deref, DerefMut};

use dashmap::DashMap;

use fluvio_controlplane_metadata::partition::ReplicaKey;

use super::replica_state::SharedLeaderState;

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
