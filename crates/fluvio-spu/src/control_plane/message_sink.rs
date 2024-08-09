use std::collections::HashSet;
use std::sync::Arc;

use async_lock::Mutex;
use fluvio_controlplane::sc_api::update_lrs::LrsRequest;
use fluvio_controlplane::sc_api::update_mirror::MirrorStatRequest;

pub type SharedStatusUpdate = Arc<StatusMessageSink>;
pub type SharedMirrorStatusUpdate = Arc<StatusMirrorMessageSink>;

/// channel used to send message to sc
#[derive(Debug)]
pub struct MessageSink<R>(Mutex<HashSet<R>>);

pub type StatusMessageSink = MessageSink<LrsRequest>;
pub type StatusMirrorMessageSink = MessageSink<MirrorStatRequest>;

impl<R> MessageSink<R>
where
    R: Eq + std::hash::Hash + Clone,
{
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(Mutex::new(HashSet::new())))
    }

    /// send lrs request sc
    /// newer entry will overwrite previous if it has not been cleared
    pub async fn send(&self, request: R) {
        let mut lock = self.0.lock().await;
        lock.replace(request);
    }

    pub async fn remove_all(&self) -> Vec<R> {
        let mut lock = self.0.lock().await;
        lock.drain().collect()
    }
}
