use std::collections::HashSet;
use std::sync::Arc;

use async_mutex::Mutex;

use fluvio_controlplane::{LrsRequest};

pub type SharedSinkMessageChannel = Arc<ScSinkMessageChannel>;

/// channel used to send message to sc
pub struct ScSinkMessageChannel(Mutex<HashSet<LrsRequest>>);

impl ScSinkMessageChannel {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(Mutex::new(HashSet::new())))
    }

    /// send lrs request sc
    /// newer entry will overwrite previous if it has not been cleared
    pub async fn send(&self, request: LrsRequest) {
        let mut lock = self.0.lock().await;
        lock.replace(request);
    }

    pub async fn remove_all(&self) -> Vec<LrsRequest> {
        let mut lock = self.0.lock().await;
        lock.drain().collect()
    }
}
