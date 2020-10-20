use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_mutex::Mutex;
use event_listener::{Event, EventListener};
use tracing::error;
use tracing::trace;

use fluvio_socket::FlvSink;
use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_controlplane::{LrsRequest, UpdateLrsRequest};
use dataplane::api::RequestMessage;

pub type SharedSinkMessageChannel = Arc<ScSinkMessageChannel>;

/// channel used to send message to sc
pub struct ScSinkMessageChannel {
    requests: Mutex<HashSet<LrsRequest>>,
    event: Event,
}

impl ScSinkMessageChannel {
    pub fn new() -> Self {
        Self {
            requests: Mutex::new(HashSet::new()),
            event: Event::new(),
        }
    }

    /// send lrs request sc
    /// newer entry will overwrite previous if it has not been cleared
    pub async fn send(&self, request: LrsRequest) {
        let mut lock = self.requests.lock().await;
        lock.insert(request);
        self.event.notify(usize::MAX);
    }

    async fn remove_all(&self) -> Vec<LrsRequest> {
        let mut lock = self.requests.lock().await;

        lock.drain().collect()
    }

    async fn len(&self) -> usize {
        self.requests.lock().await.len()
    }

    fn listen(&self) -> EventListener {
        self.event.listen()
    }
}

/// Send status back to SC
/// All status to SC must go thro this
pub struct ScMessageSinkDispatcher {
    channel: SharedSinkMessageChannel,
    sc_sink: FlvSink,
}

impl ScMessageSinkDispatcher {
    pub fn start(channel: SharedSinkMessageChannel, sc_sink: FlvSink) {
        let dispatcher = Self { channel, sc_sink };

        spawn(async move {
            dispatcher.dispatch_loop().await;
        });
    }

    async fn dispatch_loop(mut self) {
        const WAIT_TIME: Duration = Duration::from_millis(1);

        use fluvio_future::timer::sleep;
        use tokio::select;

        let mut delay = 0;
        self.send_batch_status().await;
        loop {
            let listener = self.channel.listen();
            listener.await;
            if delay < 0 {
                self.send_batch_status().await;
            } else {
            }
            // await for next

            // we are going to rate limit
            sleep(Duration::from_millis(1)).await;
            self.send_batch_status().await;
        }
    }

    async fn send_batch_status(&mut self) {
        let requests = self.channel.remove_all().await;
        let len = requests.len();

        let message = RequestMessage::new_request(UpdateLrsRequest::new(requests));

        if let Err(err) = self.sc_sink.send_request(&message).await {
            error!("error sending batch status to sc: {}", err);
        } else {
            trace!("sent replica status: {}", len);
        }
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use log::debug;
    use futures_util::future::join3;
    use event_listener::Event;

    use fluvio_future::timer::sleep;
    use fluvio_future::test_async;

    #[test_async]
    async fn test_event_lister() -> Result<(), ()> {
        let event = Arc::new(Event::new());
        let counter = Arc::new(AtomicU64::new(0));
        //  let event2 = event.clone();

        let (_a, b, c) = join3(
            async {
                sleep(Duration::from_millis(20)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                event.notify(usize::MAX);
                false
            },
            async {
                // start before notification so listener will catch notification
                sleep(Duration::from_millis(10)).await;
                // will trigger listen
                if counter.load(Ordering::SeqCst) == 0 {
                    event.listen().await;
                    debug!("waiting");
                    true
                } else {
                    false
                }
            },
            async {
                // event listener comes after notify
                // which we misses. so we need to check counter
                sleep(Duration::from_millis(30)).await;
                // we still 0 then wait
                if counter.load(Ordering::SeqCst) == 0 {
                    // this will not occur
                    event.listen().await;
                    debug!("waiting");
                    false
                } else {
                    true
                }
            },
        )
        .await;

        assert!(b);
        assert!(c);

        Ok(())
    }
}
