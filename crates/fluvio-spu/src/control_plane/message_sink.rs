use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;
use std::sync::Mutex;

use anyhow::Result;
use fluvio_controlplane::sc_api::update_lrs::LrsRequest;
use fluvio_controlplane::sc_api::update_mirror::MirrorStatRequest;
use fluvio_controlplane_metadata::mirror::{MirrorPairStatus, MirrorStatus};

pub type SharedLrsStatusUpdate = Arc<StatusLrsMessageSink>;
pub type SharedMirrorStatusUpdate = Arc<StatusMirrorMessageSink>;

/// channel used to send message to sc
#[derive(Debug)]
pub struct MessageSink<R>(Mutex<HashSet<R>>);

pub type StatusLrsMessageSink = MessageSink<LrsRequest>;
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
        let mut lock = self.0.lock().unwrap();
        lock.replace(request);
    }

    pub async fn remove_all(&self) -> Vec<R> {
        let mut lock = self.0.lock().unwrap();
        lock.drain().collect()
    }
}

impl StatusMirrorMessageSink {
    pub async fn send_status(&self, id: String, pair_status: MirrorPairStatus) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();

        let status = MirrorStatus::new_by_spu(pair_status, now as u64);
        self.send(MirrorStatRequest::new(id, status)).await;
        Ok(())
    }
}
