use std::convert::{TryFrom, TryInto};
use std::io::{Error as IoError, ErrorKind};
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, debug, instrument};
use event_listener::{Event, EventListener};
use futures_util::stream::StreamExt;

use dataplane::core::Encoder;
use dataplane::core::Decoder;
use fluvio_socket::AsyncResponse;
use fluvio_sc_schema::objects::{
    Metadata, MetadataUpdate, ObjectApiWatchRequest, ObjectApiWatchResponse, WatchResponse,
};
use fluvio_sc_schema::{AdminSpec};

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
pub struct MetadataSyncController<S: AdminSpec> {
    store: StoreContext<S::WatchResponseType>,
    shutdown: Arc<SimpleEvent>,
}

impl<S> MetadataSyncController<S>
where
    S: AdminSpec + 'static + Sync + Send,
    AsyncResponse<ObjectApiWatchRequest>: Send,
    S::WatchResponseType: Encoder + Decoder + Send + Sync,
    <S::WatchResponseType as Spec>::Status: Sync + Send + Encoder + Decoder,
    <S::WatchResponseType as Spec>::IndexKey: Display + Sync + Send,
    <WatchResponse<S> as TryFrom<ObjectApiWatchResponse>>::Error: Display + Send,
    CacheMetadataStoreObject<S::WatchResponseType>: TryFrom<Metadata<S::WatchResponseType>>,
    WatchResponse<S>: TryFrom<ObjectApiWatchResponse>,
    <Metadata<S::WatchResponseType> as TryInto<CacheMetadataStoreObject<S::WatchResponseType>>>::Error: Display,
{
    pub fn start(
        store: StoreContext<S::WatchResponseType>,
        watch_response: AsyncResponse<ObjectApiWatchRequest>,
        shutdown: Arc<SimpleEvent>,
    ) {
        use fluvio_future::task::spawn;

        let controller = Self {
            store,
            shutdown,
        };

        debug!(spec = %S::LABEL, "spawning sync controller");
        spawn(controller.dispatch_loop(watch_response));
    }

    #[instrument(
        skip(self,response),
        fields(
            spec = S::LABEL,
        )
    )]
    async fn dispatch_loop(
        mut self,
        mut response: AsyncResponse<ObjectApiWatchRequest>,
    ) {
        use tokio::select;

        debug!(spec = S::LABEL, "starting dispatch loop");

        loop {
            // check if shutdown is set
            if self.shutdown.is_set() {
                debug!(spec = S::LABEL, "shutdown exiting");
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
                                        error!(%err, "failed sync metadata updates");
                                    }
                                },
                                Err(err) => {
                                    error!(%err, "Decoding metadata update response, skipping",);
                                }
                            }

                        },
                        Some(Err(err)) => {
                            error!(%err, "Receiving response, ending");
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

        debug!(spec = %S::LABEL, "terminated", );
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
                let store_obj: Result<CacheMetadataStoreObject<S::WatchResponseType>, _> = meta.try_into();
                match store_obj {
                    Ok(obj) => {
                        objects.push(obj);
                    }
                    Err(err) => {
                        return Err(IoError::new(
                            ErrorKind::InvalidData,
                            format!("problem converting: {}", err),
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
                    let obj: Result<CacheMetadataStoreObject<S::WatchResponseType>, _> = meta.try_into();
                    obj.map(|it| (typ, it))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    IoError::new(ErrorKind::InvalidData, format!("problem converting: {}", e))
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
