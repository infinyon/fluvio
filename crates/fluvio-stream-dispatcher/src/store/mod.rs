// mod event;

pub use context::*;

pub use fluvio_stream_model::store::*;

mod context {

    use std::sync::Arc;
    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::fmt::{Display, Debug};
    use std::time::Duration;

    use fluvio_stream_model::core::MetadataItem;
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

    pub type StoreChanges<S, MetaContext = K8MetaItem> = MetadataChanges<S, MetaContext>;

    static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
        use std::env;

        let var_value = env::var("FLV_DISPATCHER_WAIT").unwrap_or_default();
        let wait_time: u64 = var_value.parse().unwrap_or(10);
        wait_time
    });

    #[derive(Debug, Clone)]
    pub struct StoreContext<S, MetaContext = K8MetaItem>
    where
        S: Spec,
        MetaContext: MetadataItem + Debug,
    {
        store: Arc<LocalStore<S, MetaContext>>,
        sender: Sender<WSAction<S, MetaContext>>,
        receiver: Receiver<WSAction<S, MetaContext>>,
        wait_time: u64,
    }

    impl<S, MetaContext> StoreContext<S, MetaContext>
    where
        S: Spec,
        MetaContext: MetadataItem,
    {
        /// create new store context
        pub fn new() -> Self {
            Self::new_with_store(LocalStore::new_shared())
        }

        /// create new store context with store
        pub fn new_with_store(store: Arc<LocalStore<S, MetaContext>>) -> Self {
            let (sender, receiver) = bounded(100);
            Self {
                store,
                sender,
                receiver,
                wait_time: *MAX_WAIT_TIME,
            }
        }

        /// set wait time out in seconds
        pub fn set_wait_time(&mut self, wait_time: u64) {
            self.wait_time = wait_time;
        }

        pub async fn send(
            &mut self,
            actions: Vec<WSAction<S, MetaContext>>,
        ) -> Result<(), SendError<WSAction<S, MetaContext>>> {
            for action in actions.into_iter() {
                self.sender.send(action).await?;
            }
            Ok(())
        }

        /// create new listener
        pub fn change_listener(&self) -> ChangeListener<S, MetaContext> {
            self.store.change_listener()
        }

        pub fn store(&self) -> &Arc<LocalStore<S, MetaContext>> {
            &self.store
        }

        pub fn receiver(&self) -> Receiver<WSAction<S, MetaContext>> {
            self.receiver.clone()
        }
    }

    impl<S, MetaContext> Default for StoreContext<S, MetaContext>
    where
        S: Spec,
        MetaContext: MetadataItem,
    {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<S, MetaContext> StoreContext<S, MetaContext>
    where
        S: Spec + PartialEq,
        MetaContext: MetadataItem,
    {
        /// Wait for the termination of the apply action object, this is similar to ```kubectl apply```
        pub async fn apply(
            &self,
            input: MetadataStoreObject<S, MetaContext>,
        ) -> Result<MetadataStoreObject<S, MetaContext>, IoError>
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
        ) -> Result<MetadataStoreObject<S, MetaContext>, IoError>
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
        ) -> Result<MetadataStoreObject<S, MetaContext>, IoError>
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

                    let mut timer = sleep(Duration::from_secs(self.wait_time));
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
                                    format!("store timed out: {} for {:?}, timer: {} secs", S::LABEL,key,self.wait_time)
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

        /// Wait for action to finish with default duration
        pub async fn wait_action(
            &self,
            key: &S::IndexKey,
            action: WSAction<S, MetaContext>,
        ) -> Result<MetadataStoreObject<S, MetaContext>, IoError>
        where
            S::IndexKey: Display,
        {
            self.wait_action_with_timeout(key, action, Duration::from_secs(self.wait_time))
                .await
        }

        /// Wait for action to finish.  There is no guarantee that the status valus has been applied.
        /// Only that status has been changed.
        ///
        /// This should only used in the imperative code such as API Server where confirmation is needed.  
        /// Controller should only use Action.
        pub async fn wait_action_with_timeout(
            &self,
            key: &S::IndexKey,
            action: WSAction<S, MetaContext>,
            timeout: Duration,
        ) -> Result<MetadataStoreObject<S, MetaContext>, IoError>
        where
            S::IndexKey: Display,
        {
            trace!("{} applying action: {:#?}", S::LABEL, action);

            let current_value = self.store.value(key).await;

            let mut spec_listener = self.change_listener();
            let mut timer = sleep(timeout);

            let debug_action = action.to_string();
            let mut loop_count: u16 = 0;
            match self.sender.send(action).await {
                Ok(_) => loop {
                    // check if we can find updates to object
                    if let Some(new_value) = self.store.value(key).await {
                        if let Some(old_value) = &current_value {
                            if new_value.is_newer(old_value) {
                                debug!("store: {}, object: {:#?}, updated", S::LABEL, key);
                                return Ok(new_value.inner_owned());
                            }
                        } else {
                            debug!("store: {}, object: {:#?}, created", S::LABEL, key);
                            return Ok(new_value.inner_owned());
                        }
                    }

                    trace!(SPEC = %S::LABEL, "waiting");

                    select! {
                        _ = &mut timer => {
                            return Err(IoError::new(
                                ErrorKind::TimedOut,
                                format!("store timed out: {debug_action} loop: {loop_count}, timer: {} ms", timeout.as_millis()),
                            ));
                        },

                        _ = spec_listener.listen() => {
                            let changes = spec_listener.sync_changes().await;
                            trace!("{} received changes: {:#?}",S::LABEL,changes);
                        }
                    }

                    loop_count += 1;
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
        pub async fn send_action(&self, action: WSAction<S, MetaContext>) {
            if let Err(err) = self.sender.send(action).await {
                error!("{}, error sending action to store: {}", S::LABEL, err);
            }
        }
    }
}
