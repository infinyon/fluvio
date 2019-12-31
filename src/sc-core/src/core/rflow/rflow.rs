use std::sync::Arc;

use futures::channel::mpsc::Receiver;

use flv_future_core::spawn;

use super::KVService;
use super::MemStore;
use super::FlowSpec;

/// Based on Kubernetes Informer
/// Maintains Cache of object
/// it populates the cache from kv store
/// if it detects diff then it fireoff controller
pub struct FlowController<S, P, KV>
where
    S: FlowSpec,
{
    store: Arc<MemStore<S, P>>,
    receiver: Receiver<bool>,
    kv_service: KV,
}

impl<S, P, KV> FlowController<S, P, KV>
where
    S: FlowSpec + Sync + Send + 'static,
    <S as FlowSpec>::Key: Sync + Send + 'static,
    P: Sync + Send + 'static,
    KV: KVService + Sync + Send + 'static,
{
    /// start the controller with ctx and receiver
    pub fn run(store: Arc<MemStore<S, P>>, kv_service: KV, receiver: Receiver<bool>) {
        let controller = Self {
            store,
            receiver,
            kv_service,
        };

        spawn(controller.inner_run());
    }

    async fn inner_run(mut self) -> Result<(), ()> {
        // let mut auth_token_stream = self.kv_service
        //     .watch_stream::<S,P>();
        Ok(())
    }
}
