use std::convert::TryInto;
use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;

use log::debug;
use log::error;

use kf_protocol::Encoder;
use kf_protocol::Decoder;
use kf_socket::AsyncResponse;
use flv_api_sc::objects::WatchRequest;
use flv_api_sc::objects::WatchResponse;
use flv_api_sc::objects::MetadataUpdate;
use flv_api_sc::objects::Metadata;
use flv_api_sc::store::MetadataStoreObject;

use crate::metadata::core::Spec;

use super::StoreContext;

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
    <S as Spec>::Status: Sync + Send,
    <S as Spec>::IndexKey: Sync + Send,
    S::IndexKey: Display,
    WatchResponse: TryInto<MetadataUpdate<S>> + Send,
    <WatchResponse as TryInto<MetadataUpdate<S>>>::Error: Display + Send,
    MetadataStoreObject<S, String>: TryFrom<Metadata<S>>,
    <Metadata<S> as TryInto<MetadataStoreObject<S, String>>>::Error: Display,
{
    pub fn start(store: StoreContext<S>, watch_response: AsyncResponse<WatchRequest>) {
        use flv_future_aio::task::spawn;

        let controller = Self { store };

        spawn(controller.dispatch_loop(watch_response));
    }

    async fn dispatch_loop(mut self, mut response: AsyncResponse<WatchRequest>) {
        use tokio::select;

        debug!("starting dispatch loop");

        loop {
            select! {
                item = response.next() => {
                    debug!("received request");

                    match item {
                        Ok(watch_response) => {
                            let update_result: Result<MetadataUpdate<S>,_> = watch_response.try_into();
                            match update_result {
                                Ok(update) => {
                                    if let Err(err) = self.process_updates(update).await {
                                        error!("{} processing updates: {}",S::LABEL,err);
                                    }

                                },
                                Err(err) => {
                                    error!("Error decoding metadata {} update response: {}",S::LABEL,err);
                                }
                            }
                        },
                        Err(err) => {
                            error!("{} error receiving, end, {}",S::LABEL,err);
                            break;
                        }
                    }

                }
            }
        }
    }

    async fn process_updates(&mut self, updates: MetadataUpdate<S>) -> Result<(), IoError> {
        
        if updates.all.len() > 0 {
            debug!(
                "processing {}, sync all items: {}",
                S::LABEL,
                updates.all.len()
            );
            let mut objects: Vec<MetadataStoreObject<S, String>> = vec![];
            for meta in updates.all.into_iter() {
                let store_obj: Result<MetadataStoreObject<S, String>, _> = meta.try_into();
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
            self.store.notify();
        }

        Ok(())
    }
}
