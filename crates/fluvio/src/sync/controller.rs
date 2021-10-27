use std::io::Error as IoError;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, debug, instrument};
use event_listener::{Event, EventListener};

use dataplane::core::Encoder;
use dataplane::core::Decoder;
use fluvio_socket::AsyncResponse;
use fluvio_sc_schema::objects::{WatchRequest, MetadataUpdate, Metadata};
use fluvio_sc_schema::AdminSpec;

use crate::metadata::core::Spec;

use super::StoreContext;
use super::CacheMetadataStoreObject;
use crate::metadata::store::actions::LSUpdate;
use fluvio_sc_schema::message::MsgType;

pub struct SimpleEvent {
    flag: AtomicBool,
    event: Event,
}

impl SimpleEvent {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            flag: AtomicBool::new(false),
            event: Event::new(),
        })
    }
    // is flag set
    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub fn listen(&self) -> EventListener {
        self.event.listen()
    }

    pub fn notify(&self) {
        self.event.notify(usize::MAX);
    }
}

/// Synchronize metadata from SC
pub struct MetadataSyncController<S>
where
    S: AdminSpec,
{
    store: StoreContext<S::WatchResponseType>,
    shutdown: Arc<SimpleEvent>,
}

impl<S> MetadataSyncController<S>
where
    S: AdminSpec + 'static,
    AsyncResponse<WatchRequest<S>>: Send,
    S::WatchResponseType: Encoder + Decoder + Send + Sync,
    <S::WatchResponseType as Spec>::Status: Sync + Send + Encoder + Decoder,
    <S::WatchResponseType as Spec>::IndexKey: Display + Sync + Send,
    CacheMetadataStoreObject<S::WatchResponseType>: From<Metadata<S::WatchResponseType>>,
{
    pub fn start(
        store: StoreContext<S::WatchResponseType>,
        watch_response: AsyncResponse<WatchRequest<S>>,
        shutdown: Arc<SimpleEvent>,
    ) {
        use fluvio_future::task::spawn;

        let controller = Self { store, shutdown };

        debug!(spec = %S::LABEL, "spawning sync controller");
        spawn(controller.dispatch_loop(watch_response));
    }

    #[instrument(
        skip(self,response),
        fields(
            spec = S::LABEL,
        )
    )]
    async fn dispatch_loop(mut self, mut response: AsyncResponse<WatchRequest<S>>) {
        use tokio::select;

        debug!("{} starting dispatch loop", S::LABEL);

        loop {
            // check if shutdown is set
            if self.shutdown.is_set() {
                debug!("{} shutdown exiting", S::LABEL);
                break;
            }

            select! {
                _ = self.shutdown.listen() => {
                    break;
                }

                item = response.next() => {
                    debug!(spec = %S::LABEL,"received request");

                    match item {
                        Some(Ok(watch_response)) => {
                            let update_result = MetadataUpdate<S::WatchResponseType> = watch_response.into();
                            if let Err(err) = self.sync_metadata(update).await {
                                error!("Processing updates: {}", err);
                            }
                        },
                        Some(Err(err)) => {
                            error!("Receiving response, ending: {}", err);
                            break;
                        },
                        None => {
                            debug!("No more items to receive from stream!");
                            break;
                        }
                    }
                }
            }
        }

        debug!("{} terminated", S::LABEL);
    }

    // process updates from sc
    #[instrument(
        skip(self, updates),
        fields(
            epoch = updates.epoch,
            spec = %S::LABEL,
        ),
    )]
    async fn sync_metadata(
        &mut self,
        updates: MetadataUpdate<S::WatchResponseType>,
    ) -> Result<(), IoError> {
        // Full sync
        if !updates.all.is_empty() {
            debug!(
                count = updates.all.len(),
                "Received full sync, setting store objects:"
            );
            let mut objects: Vec<CacheMetadataStoreObject<S::WatchResponseType>> = vec![];
            for meta in updates.all.into_iter() {
                let store_obj: CacheMetadataStoreObject<S::WatchResponseType> = meta.into();
                objects.push(store_obj);
            }
            self.store.store().sync_all(objects).await;
            return Ok(());
        }

        // Partial sync
        if !updates.changes.is_empty() {
            debug!(
                count = updates.changes.len(),
                "Received partial sync, updating store objects:"
            );
            let changes = updates
                .changes
                .into_iter()
                .map(|msg| {
                    let (meta, typ) = (msg.content, msg.header);
                    let obj: CacheMetadataStoreObject<S::WatchResponseType> = meta.into();
                    (typ, obj)
                })
                .into_iter()
                // .map(|it| LSUpdate::Mod(it))
                .map(|(typ, obj)| match typ {
                    MsgType::UPDATE => LSUpdate::Mod(obj),
                    MsgType::DELETE => LSUpdate::Delete(obj.key),
                })
                .collect::<Vec<_>>();

            self.store.store().apply_changes(changes).await;
            return Ok(());
        }

        debug!("No metadata updates received. Not syncing store");
        Ok(())
    }
}
