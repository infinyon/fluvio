// mod event;

pub use context::*;

pub use fluvio_stream_model::store::*;

mod context {

    use std::sync::Arc;
    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::fmt::Display;
    use std::time::Duration;

    use tracing::error;
    use async_channel::{Sender, Receiver, bounded, SendError};
    use once_cell::sync::Lazy;
    use tokio::select;
    use tracing::{debug, trace};

    use fluvio_future::timer::sleep;

    use crate::actions::WSAction;
    use crate::store::k8::K8MetaItem;
    use crate::core::Spec;

    use super::MetadataStoreObject;
    use super::{LocalStore, ChangeListener, MetadataChanges};

    pub type K8ChangeListener<S> = ChangeListener<S, K8MetaItem>;

    pub type StoreChanges<S> = MetadataChanges<S, K8MetaItem>;

    static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
        use std::env;

        let var_value = env::var("FLV_DISPATCHER_WAIT").unwrap_or_default();
        let wait_time: u64 = var_value.parse().unwrap_or(10);
        wait_time
    });

    #[derive(Debug, Clone)]
    pub struct StoreContext<S>
    where
        S: Spec,
    {
        store: Arc<LocalStore<S, K8MetaItem>>,
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

        /// create new listener
        pub fn change_listener(&self) -> K8ChangeListener<S> {
            self.store.change_listener()
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
        /// Wait for the termination of the apply action object, this is similar to ```kubectl apply```
        pub async fn apply(
            &self,
            input: MetadataStoreObject<S, K8MetaItem>,
        ) -> Result<MetadataStoreObject<S, K8MetaItem>, IoError>
        where
            S::IndexKey: Display,
        {
            let key: S::IndexKey = input.key_owned();
            debug!("{}: applying: {}", S::LABEL, key);

            self.wait_action(&key, WSAction::Apply(input)).await
        }

        /// Wait for creation/update of spec.  There is no guarantee that this spec has been applied.
        /// Only that spec has been changed.
        ///
        /// This should only used in the imperative code such as API Server where confirmation is needed.  
        /// Controller should only use Action.
        pub async fn create_spec(
            &self,
            key: S::IndexKey,
            spec: S,
        ) -> Result<MetadataStoreObject<S, K8MetaItem>, IoError>
        where
            S::IndexKey: Display,
        {
            debug!("{}: creating store: {}", S::LABEL, key);

            self.wait_action(&key, WSAction::UpdateSpec((key.clone(), spec)))
                .await
        }

        /// Wait for status update.  There is no guarantee that this status valus has been applied.
        /// Only that status has been changed.
        ///
        /// This should only used in the imperative code such as API Server where confirmation is needed.  
        /// Controller should only use Action.
        pub async fn update_status(
            &self,
            key: S::IndexKey,
            status: S::Status,
        ) -> Result<MetadataStoreObject<S, K8MetaItem>, IoError>
        where
            S::IndexKey: Display,
        {
            debug!("{}: updating status: {}", S::LABEL, key);

            self.wait_action(&key, WSAction::UpdateStatus((key.clone(), status)))
                .await
        }

        /// Wait for object deletion.  
        ///
        /// This should only used in the imperative code such as API Server where confirmation is needed.  
        /// Controller should only use Action.
        pub async fn delete(&self, key: S::IndexKey) -> Result<(), IoError> {
            match self.sender.send(WSAction::Delete(key.clone())).await {
                Ok(_) => {
                    // wait for object created in the store

                    let mut timer = sleep(Duration::from_secs(*MAX_WAIT_TIME));
                    let mut spec_listener = self.change_listener();
                    loop {
                        // check if we can find old object
                        if !self.store.contains_key(&key).await {
                            debug!("store: {}, object: {:#?}, has been deleted", S::LABEL, key);
                            return Ok(());
                        }

                        debug!("{} store, waiting for delete event", S::LABEL);

                        select! {
                            _ = &mut timer => {
                                return Err(IoError::new(
                                    ErrorKind::TimedOut,
                                    format!("store timed out: {} for {:?}", S::LABEL,key)
                                ));
                            },
                            _ = spec_listener.listen() => {

                                let changes = spec_listener.sync_changes().await;
                                trace!("{} received changes: {:#?}",S::LABEL,changes);
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

        /// Wait for action to finish.  There is no guarantee that the status valus has been applied.
        /// Only that status has been changed.
        ///
        /// This should only used in the imperative code such as API Server where confirmation is needed.  
        /// Controller should only use Action.
        pub async fn wait_action(
            &self,
            key: &S::IndexKey,
            action: WSAction<S>,
        ) -> Result<MetadataStoreObject<S, K8MetaItem>, IoError>
        where
            S::IndexKey: Display,
        {
            trace!("{} applying action: {:#?}", S::LABEL, action);

            let current_value = self.store.value(key).await;

            let mut spec_listener = self.change_listener();
            let mut timer = sleep(Duration::from_secs(*MAX_WAIT_TIME));

            match self.sender.send(action).await {
                Ok(_) => loop {
                    if let Some(new_value) = self.store.value(&key).await {
                        if let Some(old_value) = &current_value {
                            if new_value.is_newer(&old_value) {
                                debug!("store: {}, object: {:#?}, updated", S::LABEL, key);
                                return Ok(new_value.inner_owned());
                            }
                        } else {
                            debug!("store: {}, object: {:#?}, created", S::LABEL, key);
                            return Ok(new_value.inner_owned());
                        }
                    }

                    trace!("{} store, waiting for create event", S::LABEL);

                    select! {
                        _ = &mut timer => {
                            return Err(IoError::new(
                                ErrorKind::TimedOut,
                                format!("store timed out: {} for {:?}", S::LABEL, key),
                            ));
                        },

                        _ = spec_listener.listen() => {
                            let changes = spec_listener.sync_changes().await;
                            trace!("{} received changes: {:#?}",S::LABEL,changes);
                        }
                    }
                },
                Err(err) => {
                    error!("{}, error sending to store: {}", S::LABEL, err);
                    Err(IoError::new(
                        ErrorKind::UnexpectedEof,
                        format!("not able to send out: {} for {}", S::LABEL, key),
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
