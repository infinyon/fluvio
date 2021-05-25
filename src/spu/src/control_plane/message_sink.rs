use std::collections::HashSet;
use std::sync::Arc;
use tracing::instrument;

use async_lock::Mutex;

use fluvio_controlplane::{LrsRequest};

pub type SharedStatusUpdate = Arc<StatusMessageSink>;

/// channel used to send message to sc
#[derive(Debug)]
pub struct StatusMessageSink(Mutex<HashSet<LrsRequest>>);

impl StatusMessageSink {
    #[instrument]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self(Mutex::new(HashSet::new())))
    }

    /// send lrs request sc
    /// newer entry will overwrite previous if it has not been cleared
    #[instrument(skip(self, request))]
    pub async fn send(&self, request: LrsRequest) {
        let mut lock = self.0.lock().await;
        lock.replace(request);
    }

    #[instrument(skip(self))]
    pub async fn remove_all(&self) -> Vec<LrsRequest> {
        let mut lock = self.0.lock().await;
        lock.drain().collect()
    }
}
