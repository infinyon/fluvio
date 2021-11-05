mod k8_dispatcher;
mod k8_ws_service;

pub use k8_dispatcher::*;
pub use k8_ws_service::*;

pub mod memory {

    use std::fmt::Display;

    use fluvio_future::task::spawn;
    use fluvio_stream_model::{
        core::Spec,
        store::{actions::LSUpdate, memory::MemoryMeta},
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
                        match action {
                            WSAction::UpdateStatus((key, new_status)) => {
                                // get existing object
                                debug!("update status: {:#?}", new_status);
                                if let Some(old) = self.ctx.store().value(&key).await {
                                    debug!("old status: {:#?}", old.status);
                                    let mut new_value = old.inner_owned().next_rev();
                                    new_value.status = new_status;
                                    if let Some(_status) = self
                                        .ctx
                                        .store()
                                        .apply_changes(vec![LSUpdate::Mod(new_value)])
                                        .await
                                    {
                                        debug!("changed");
                                    } else {
                                        debug!("no change");
                                    }
                                }
                            }
                            _ => {
                                debug!("not yet implemented");
                            }
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
