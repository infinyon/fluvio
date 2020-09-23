mod controller;
mod store;

pub use store::*;
pub use context::*;

mod context {

    use std::sync::Arc;
    use std::fmt::Display;

    use tracing::debug;
    use tracing::warn;

    use event_listener::Event;
    use event_listener::EventListener;
    use async_rwlock::RwLockReadGuard;

    use crate::FluvioError;
    use crate::metadata::core::Spec;
    use crate::metadata::store::LocalStore;
    use crate::metadata::store::EpochMap;
    use crate::metadata::store::MetadataStoreObject;
    use crate::metadata::spu::SpuSpec;

    #[derive(Debug, Clone)]
    pub struct StoreContext<S>
    where
        S: Spec,
    {
        store: Arc<LocalStore<S, String>>,
        event: Arc<Event>,
    }

    impl<S> StoreContext<S>
    where
        S: Spec,
    {
        pub fn new() -> Self {
            Self {
                store: LocalStore::new_shared(),
                event: Arc::new(Event::new()),
            }
        }

        pub fn store(&self) -> &Arc<LocalStore<S, String>> {
            &self.store
        }

        pub fn listen(&self) -> EventListener {
            self.event.listen()
        }

        pub fn notify(&self) {
            self.event.notify(usize::MAX);
        }

        pub async fn lookup_by_key(
            &self,
            key: &S::IndexKey,
        ) -> Result<MetadataStoreObject<S, String>, FluvioError>
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
        ) -> Result<MetadataStoreObject<S, String>, FluvioError>
        where
            S: 'static,
            S::IndexKey: Display,
            F: Fn(
                RwLockReadGuard<'a, EpochMap<S::IndexKey, MetadataStoreObject<S, String>>>,
            ) -> Option<MetadataStoreObject<S, String>>,
        {
            use std::time::Instant;
            use std::time::Duration;
            use std::io::Error as IoError;
            use std::io::ErrorKind;

            use tokio::select;
            use fluvio_future::timer::sleep;

            const TIMER_DURATION: u64 = 180;

            let mut time_left = Duration::from_secs(TIMER_DURATION);

            loop {
                debug!("{} checking to see if exists", S::LABEL);
                if let Some(value) = search(self.store().read().await) {
                    debug!("{} found value", S::LABEL);
                    return Ok(value);
                } else {
                    debug!("{} value not found, waiting", S::LABEL);
                    let current_time = Instant::now();

                    select! {

                        _ = sleep(time_left) => {
                            warn!("store {}: look up timeout expired",S::LABEL);
                            return Err(FluvioError::IoError(IoError::new(
                                ErrorKind::TimedOut,
                                format!("{} store lookup failed due to timeout",S::LABEL),
                            )))
                        },

                        _ = self.listen() => {

                            time_left -= current_time.elapsed();
                            debug!("{} store updated",S::LABEL);
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
        ) -> Result<MetadataStoreObject<SpuSpec, String>, FluvioError> {
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
