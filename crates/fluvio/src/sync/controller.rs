use std::convert::{TryFrom, TryInto};
use std::io::{Error as IoError, ErrorKind};
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, debug, instrument};
use event_listener::{Event, EventListener};
use futures_util::stream::StreamExt;
use anyhow::Result;

use fluvio_protocol::Encoder;
use fluvio_protocol::Decoder;
use fluvio_socket::AsyncResponse;
use fluvio_sc_schema::objects::{
    Metadata, MetadataUpdate, ObjectApiWatchRequest, ObjectApiWatchResponse, WatchResponse,
};
use fluvio_sc_schema::AdminSpec;

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
pub struct MetadataSyncController<S: AdminSpec> {
    store: StoreContext<S>,
    shutdown: Arc<SimpleEvent>,
}

impl<S> MetadataSyncController<S>
where
    S: AdminSpec + 'static + Sync + Send,
    AsyncResponse<ObjectApiWatchRequest>: Send,
    S: Encoder + Decoder + Send + Sync,
    S::Status: Sync + Send + Encoder + Decoder,
    S::IndexKey: Display + Sync + Send,
    <WatchResponse<S> as TryFrom<ObjectApiWatchResponse>>::Error: Display + Send,
    CacheMetadataStoreObject<S>: TryFrom<Metadata<S>>,
    WatchResponse<S>: TryFrom<ObjectApiWatchResponse>,
    <Metadata<S> as TryInto<CacheMetadataStoreObject<S>>>::Error: Display,
{
    pub fn start(
        store: StoreContext<S>,
        watch_response: AsyncResponse<ObjectApiWatchRequest>,
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
    async fn dispatch_loop(mut self, mut response: AsyncResponse<ObjectApiWatchRequest>) {
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
                            let update_result: Result<WatchResponse<S>,_> = watch_response.try_into();
                            match update_result {
                                Ok(update) => {
                                    if let Err(err) = self.sync_metadata(update.inner()).await {
                                        error!("Processing updates: {}", err);
                                    }
                                },
                                Err(err) => {
                                    error!("Decoding metadata update response, skipping: {}", err);
                                }
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
    async fn sync_metadata(&mut self, updates: MetadataUpdate<S>) -> Result<(), IoError> {
        // Full sync
        if !updates.all.is_empty() {
            debug!(
                count = updates.all.len(),
                "Received full sync, setting store objects:"
            );
            let mut objects: Vec<CacheMetadataStoreObject<S>> = vec![];
            for meta in updates.all.into_iter() {
                let store_obj: Result<CacheMetadataStoreObject<S>, _> = meta.try_into();
                match store_obj {
                    Ok(obj) => {
                        objects.push(obj);
                    }
                    Err(err) => {
                        return Err(IoError::new(
                            ErrorKind::InvalidData,
                            format!("problem converting: {err}"),
                        ));
                    }
                }
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
                    let obj: Result<CacheMetadataStoreObject<S>, _> = meta.try_into();
                    obj.map(|it| (typ, it))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    IoError::new(ErrorKind::InvalidData, format!("problem converting: {e}"))
                })?
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
