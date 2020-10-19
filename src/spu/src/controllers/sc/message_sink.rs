use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_mutex::Mutex;
use tracing::error;
use tracing::trace;

use fluvio_socket::FlvSink;
use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_controlplane::{ LrsRequest, UpdateLrsRequest };
use dataplane::api::RequestMessage;

pub type SharedSinkMessageChannel = Arc<ScSinkMessageChannel>;

/// channel used to send message to sc
pub struct ScSinkMessageChannel(Mutex<HashSet<LrsRequest>>);

impl ScSinkMessageChannel {
    pub fn new() -> Self {
        Self(Mutex::new(HashSet::new()))
    }

    /// send lrs request sc
    /// newer entry will overwrite previous if it has not been cleared
    pub async fn send(&self, request: LrsRequest) {
        let mut lock = self.0.lock().await;
        lock.insert(request);
    }

    pub async fn remove_all(&self) -> Vec<LrsRequest> {
        let mut lock = self.0.lock().await;

        lock.drain().collect()
    }
}

/// Send status back to SC
/// All status to SC must go thro this
pub struct ScMessageSinkDispatcher {
    channel: SharedSinkMessageChannel,
    sc_sink: FlvSink
}

impl ScMessageSinkDispatcher {
    pub fn start(channel: SharedSinkMessageChannel,sc_sink: FlvSink) {
        let dispatcher = Self { 
            channel,
            sc_sink 
        };

        spawn(async move {
            dispatcher.dispatch_loop().await;
        });
    }

    async fn dispatch_loop(mut self) {
        const WAIT_TIME: Duration = Duration::from_millis(1);

        use fluvio_future::timer::sleep;

        loop {
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
            error!(
                "error sending batch status to sc: {}",
                err
            );
        } else {
            trace!(
                "sent replica status: {}",
                len
            );
        }
    }
}
