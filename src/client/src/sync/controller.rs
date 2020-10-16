use std::convert::TryInto;
use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;

use tracing::{error, debug, instrument};
use futures_util::stream::StreamExt;

use dataplane::core::Encoder;
use dataplane::core::Decoder;
use fluvio_socket::AsyncResponse;
use fluvio_sc_schema::objects::WatchRequest;
use fluvio_sc_schema::objects::WatchResponse;
use fluvio_sc_schema::objects::MetadataUpdate;
use fluvio_sc_schema::objects::Metadata;

use crate::metadata::core::Spec;

use super::StoreContext;
use super::CacheMetadataStoreObject;

///
pub struct MetadataSyncController<S>
where
    S: Spec,
{
    store: StoreContext<S>,
}

impl<S> MetadataSyncController<S>
where
    S: Spec + Encoder + Decoder + Sync + Send + 'static,
    <S as Spec>::Status: Sync + Send + Encoder + Decoder,
    <S as Spec>::IndexKey: Sync + Send,
    S::IndexKey: Display,
    WatchResponse: TryInto<MetadataUpdate<S>> + Send,
    <WatchResponse as TryInto<MetadataUpdate<S>>>::Error: Display + Send,
    CacheMetadataStoreObject<S>: TryFrom<Metadata<S>>,
    <Metadata<S> as TryInto<CacheMetadataStoreObject<S>>>::Error: Display,
{
    pub fn start(store: StoreContext<S>, watch_response: AsyncResponse<WatchRequest>) {
        use fluvio_future::task::spawn;

        let controller = Self { store };

        spawn(controller.dispatch_loop(watch_response));
    }

    #[instrument(
        skip(self, response),
        fields(spec = &*S::LABEL)
    )]
    async fn dispatch_loop(mut self, mut response: AsyncResponse<WatchRequest>) {
        use tokio::select;

        debug!("starting dispatch loop");

        loop {
            select! {
                item = response.next() => {
                    debug!("received request");

                    match item {
                        Some(Ok(watch_response)) => {
                            let update_result: Result<MetadataUpdate<S>,_> = watch_response.try_into();
                            match update_result {
                                Ok(update) => {
                                    if let Err(err) = self.process_updates(update).await {
                                        error!("processing updates: {}", err);
                                    }

                                },
                                Err(err) => {
                                    error!("Error decoding metadata {} update response: {}",S::LABEL,err);
                                }
                            }
                        },
                        Some(Err(err)) => {
                            error!("error receiving, end, {}", err);
                            break;
                        },
                        None => {
                            error!("No more items to receive from stream!")
                        }
                    }
                }
            }
        }
    }

    async fn process_updates(&mut self, updates: MetadataUpdate<S>) -> Result<(), IoError> {
        if !updates.all.is_empty() {
            debug!(
                "processing {}, sync all items: {}",
                S::LABEL,
                updates.all.len()
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
                            format!("problem converting: {}", err),
                        ));
                    }
                }
            }
            self.store.store().sync_all(objects).await;
            self.store.notify_spec_changes();
            self.store.notify_status_changes();
        }

        Ok(())
    }
}
