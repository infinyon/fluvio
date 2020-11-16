mod controller;
mod store;

pub use store::*;
pub use context::*;

mod context {

    use std::sync::Arc;
    use std::fmt::Display;

    use tracing::debug;

    use event_listener::Event;
    use event_listener::EventListener;
    use async_rwlock::RwLockReadGuard;

    use crate::FluvioError;
    use crate::metadata::core::Spec;
    use crate::metadata::store::LocalStore;
    use crate::metadata::store::DualEpochMap;
    use crate::metadata::store::MetadataStoreObject;
    use crate::metadata::spu::SpuSpec;
    use crate::metadata::core::MetadataItem;

    pub(crate) type CacheMetadataStoreObject<S> = MetadataStoreObject<S, AlwaysNewContext>;

    /// context that always updates
    #[derive(Debug, Default, Clone, PartialEq)]
    pub struct AlwaysNewContext {}

    impl MetadataItem for AlwaysNewContext {
        type UId = u64;

        fn uid(&self) -> &Self::UId {
            &0
        }

        fn is_newer(&self, _another: &Self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    pub struct StoreContext<S>
    where
        S: Spec,
    {
        store: Arc<LocalStore<S, AlwaysNewContext>>,
        spec_event: Arc<Event>,
        status_event: Arc<Event>,
    }

    impl<S> StoreContext<S>
    where
        S: Spec,
    {
        pub fn new() -> Self {
            Self {
                store: LocalStore::new_shared(),
                spec_event: Arc::new(Event::new()),
                status_event: Arc::new(Event::new()),
            }
        }

        pub fn store(&self) -> &Arc<LocalStore<S, AlwaysNewContext>> {
            &self.store
        }

        pub fn listen(&self) -> EventListener {
            self.spec_event.listen()
        }

        #[allow(unused)]
        pub fn status_listen(&self) -> EventListener {
            self.status_event.listen()
        }

        /// notify changes to specs
        pub fn notify_spec_changes(&self) {
            self.spec_event.notify(usize::MAX);
        }

        /// notify changes to status
        pub fn notify_status_changes(&self) {
            self.status_event.notify(usize::MAX);
        }

        /// look up object by index key
        #[allow(unused)]
        pub async fn try_lookup_by_key(
            &self,
            key: &S::IndexKey,
        ) -> Option<CacheMetadataStoreObject<S>> {
            let read_lock = self.store.read().await;
            read_lock.get(key).map(|value| value.inner().clone())
        }

        pub async fn lookup_by_key(
            &self,
            key: &S::IndexKey,
        ) -> Result<CacheMetadataStoreObject<S>, FluvioError>
        where
            S: 'static,
            S::IndexKey: Display,
        {
            debug!("lookup for {} key: {}", S::LABEL, key);
            self.lookup_and_wait(|g| g.get(key).map(|v| v.inner().clone()))
                .await
        }

        /// look up value for key, if it doesn't exists, wait with max timeout
        pub async fn lookup_and_wait<'a, F>(
            &'a self,
            search: F,
        ) -> Result<CacheMetadataStoreObject<S>, FluvioError>
        where
            S: 'static,
            S::IndexKey: Display,
            F: Fn(
                RwLockReadGuard<'a, DualEpochMap<S::IndexKey, CacheMetadataStoreObject<S>>>,
            ) -> Option<CacheMetadataStoreObject<S>>,
        {
            use std::time::Duration;
            use std::io::Error as IoError;
            use std::io::ErrorKind;

            use tokio::select;
            use fluvio_future::timer::sleep;

            const TIMER_DURATION: u64 = 300;

            let mut timer = (Duration::from_millis(TIMER_DURATION));

            loop {
                debug!( SPEC = S::LABEL, "checking to see if exists");
                if let Some(value) = search(self.store().read().await) {
                    debug!( SPEC = S::LABEL, "found value");
                    return Ok(value);
                } else {
                    debug!(SPEC = S::LABEL, "value not found, waiting");
                    
                    select! {

                        _ = &mut timer => {
                            debug!( SPEC = S::LABEL, "store look up timeout expired");
                            return Err(IoError::new(
                                ErrorKind::TimedOut,
                                format!("{} store lookup failed due to timeout",S::LABEL),
                            ).into())
                        },

                        _ = self.listen() => {

                            debug!( SPEC = S::LABEL, "store updated");
                        }

                    }
                }
            }
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

    impl StoreContext<SpuSpec> {
        pub async fn look_up_by_id(
            &self,
            id: i32,
        ) -> Result<CacheMetadataStoreObject<SpuSpec>, FluvioError> {
            self.lookup_and_wait(|g| {
                for spu in g.values() {
                    if spu.spec.id == id {
                        return Some(spu.inner().clone());
                    }
                }
                None
            })
            .await
        }
    }
}
