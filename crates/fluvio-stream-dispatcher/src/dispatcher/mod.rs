mod k8_dispatcher;
mod k8_ws_service;

pub use k8_dispatcher::*;
pub use k8_ws_service::*;

pub mod memory {

    use std::fmt::Display;

    use fluvio_future::task::spawn;
    use fluvio_stream_model::{
        core::Spec,
        store::{MetadataStoreObject, actions::LSUpdate, memory::MemoryMeta},
    };
    use tracing::{debug, error, trace};

    use crate::{actions::WSAction, store::StoreContext};

    /// directly dispatch values to memory store
    pub struct MemoryDispatcher<S>
    where
        S: Spec,
    {
        ctx: StoreContext<S, MemoryMeta>,
    }

    impl<S> MemoryDispatcher<S>
    where
        S: Spec + Sync + Send + 'static,
        S::Status: Sync + Send + 'static,
        <S as Spec>::IndexKey: Sync + Send + Display + 'static,
    {
        pub fn start(ctx: StoreContext<S, MemoryMeta>) {
            let dispatcher = Self { ctx };

            spawn(dispatcher.out_loop());
        }

        async fn out_loop(self) {
            let ws_receiver = self.ctx.receiver();

            loop {
                trace!("dispatcher waiting");

                let msg = ws_receiver.recv().await;

                match msg {
                    Ok(action) => {
                        debug!("store: received ws action: {:#?}", action);
                        match action {
                            WSAction::UpdateStatus((key, status)) => {
                                // get existing object
                                if let Some(old) = self.ctx.store().value(&key).await {
                                    let _ = self
                                        .ctx
                                        .store()
                                        .apply_changes(vec![LSUpdate::Mod(
                                            MetadataStoreObject::new(
                                                old.key_owned(),
                                                old.spec.clone(),
                                                status,
                                            ),
                                        )])
                                        .await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        error!("WS channel error: {}", err);
                    }
                }
            }
        }
    }
}
