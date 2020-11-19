pub use context::*;

pub use fluvio_stream_model::store::*;

mod context {

    use std::sync::Arc;
    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::fmt::Display;

    use tracing::error;
    use event_listener::{Event, EventListener};
    use async_channel::{Sender, Receiver, bounded, SendError};

    use crate::actions::WSAction;
    use crate::store::k8::K8MetaItem;

    use super::MetadataStoreObject;
    use super::LocalStore;
    use crate::core::Spec;

    #[derive(Debug, Clone)]
    pub struct StoreContext<S>
    where
        S: Spec,
    {
        store: Arc<LocalStore<S, K8MetaItem>>,
        spec_event: Arc<Event>,
        status_event: Arc<Event>,
        sender: Sender<WSAction<S>>,
        receiver: Receiver<WSAction<S>>,
    }

    impl<S> StoreContext<S>
    where
        S: Spec,
    {
        pub fn new() -> Self {
            let (sender, receiver) = bounded(100);
            Self {
                store: LocalStore::new_shared(),
                spec_event: Arc::new(Event::new()),
                status_event: Arc::new(Event::new()),
                sender,
                receiver,
            }
        }

        pub async fn send(
            &mut self,
            actions: Vec<WSAction<S>>,
        ) -> Result<(), SendError<WSAction<S>>> {
            for action in actions.into_iter() {
                self.sender.send(action).await?;
            }
            Ok(())
        }

        pub fn notify_spec_changes(&self) {
            self.spec_event.notify(usize::MAX);
        }

        pub fn notify_status_changes(&self) {
            self.status_event.notify(usize::MAX);
        }

        /// listen to spec
        pub fn spec_listen(&self) -> EventListener {
            self.spec_event.listen()
        }

        /// list to status
        pub fn status_listen(&self) -> EventListener {
            self.status_event.listen()
        }

        pub fn store(&self) -> &Arc<LocalStore<S, K8MetaItem>> {
            &self.store
        }

        pub fn receiver(&self) -> Receiver<WSAction<S>> {
            self.receiver.clone()
        }
    }

    impl<S> Default for StoreContext<S>
    where
        S: Spec,
    {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<S> StoreContext<S>
    where
        S: Spec + PartialEq,
    {
        /// wait for creation of new metadata
        /// there is 5 second time out
        pub async fn create_spec(
            &self,
            key: S::IndexKey,
            spec: S,
        ) -> Result<MetadataStoreObject<S, K8MetaItem>, IoError>
        where
            S::IndexKey: Display,
        {
            use std::time::Duration;
            use std::time::Instant;

            use once_cell::sync::Lazy;
            use tokio::select;
            use tracing::debug;
            use fluvio_future::timer::sleep;

            static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
                use std::env;

                let var_value = env::var("FLV_DISPATCHER_WAIT").unwrap_or_default();
                let wait_time: u64 = var_value.parse().unwrap_or(10);
                wait_time
            });

            const POLL_TIME: u64 = 1;

            debug!("{}: sending WS action to store: {}", S::LABEL, key);
            let action = WSAction::UpdateSpec((key.clone(), spec));

            match self.sender.send(action).await {
                Ok(_) => {
                    // wait for object created in the store

                    let instant = Instant::now();
                    let max_wait = Duration::from_secs(*MAX_WAIT_TIME);
                    loop {
                        debug!("{} store, waiting for store event", S::LABEL);

                        if let Some(value) = self.store.value(&key).await {
                            debug!("store: {}, object: {:#?}, created", S::LABEL, key);
                            return Ok(value.inner_owned());
                        } else {
                            // check if total time expired
                            if instant.elapsed() > max_wait {
                                return Err(IoError::new(
                                    ErrorKind::TimedOut,
                                    format!("store timed out: {} for {:?}", S::LABEL, key),
                                ));
                            }
                        }

                        select! {
                            _ = sleep(Duration::from_secs(POLL_TIME)) => {
                                debug!("{} store, didn't receive wait,exiting,continue waiting",S::LABEL);
                            },
                            _ = self.spec_listen() => {
                                debug!("{} store, received updates",S::LABEL);
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("{}, error sending to store: {}", S::LABEL, err);
                    Err(IoError::new(
                        ErrorKind::UnexpectedEof,
                        format!("not able to send out: {} for {}", S::LABEL, key),
                    ))
                }
            }
        }

        /// wait for delete of metadata object
        /// there is 5 second time out
        pub async fn delete(&self, key: S::IndexKey) -> Result<(), IoError> {
            use std::time::Duration;
            use std::time::Instant;

            use tokio::select;
            use tracing::debug;
            use tracing::warn;
            use fluvio_future::timer::sleep;

            const MAX_WAIT_TIME: u64 = 5;

            let action = WSAction::Delete(key.clone());
            match self.sender.send(action).await {
                Ok(_) => {
                    // wait for object created in the store

                    let instant = Instant::now();
                    let max_wait = Duration::from_secs(MAX_WAIT_TIME);
                    loop {
                        debug!("{} store, waiting for store event", S::LABEL);

                        select! {
                            _ = sleep(Duration::from_secs(MAX_WAIT_TIME)) => {
                                warn!("{} store, didn't receive wait,exiting",S::LABEL);
                                return Err(IoError::new(
                                    ErrorKind::TimedOut,
                                    format!("store timed out: {} for {:?}", S::LABEL,key)
                                ));
                            },
                            _ = self.spec_listen() => {
                                // check if we can find old object
                                if !self.store.contains_key(&key).await {
                                    debug!("store: {}, object: {:#?}, has been deleted",S::LABEL,key);
                                    return Ok(())
                                } else {
                                    // check if total time expired
                                    if instant.elapsed() > max_wait {
                                        return Err(IoError::new(
                                            ErrorKind::TimedOut,
                                            format!("store timed out: {} for {:?}", S::LABEL,key)
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("{}, error sending to store: {}", S::LABEL, err);
                    Err(IoError::new(
                        ErrorKind::UnexpectedEof,
                        format!("not able to send out: {} for {:?}", S::LABEL, key),
                    ))
                }
            }
        }

        /// send action
        pub async fn send_action(&self, action: WSAction<S>) {
            if let Err(err) = self.sender.send(action).await {
                error!("{}, error sending action to store: {}", S::LABEL, err);
            }
        }
    }
}
