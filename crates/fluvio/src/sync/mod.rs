mod controller;
mod store;

pub use store::*;
pub use context::*;

mod context {

    use std::sync::Arc;
    use std::fmt::Display;
    use std::io::Error as IoError;

    use tracing::{debug, instrument};
    use async_rwlock::RwLockReadGuard;
    use once_cell::sync::Lazy;

    use crate::FluvioError;
    use crate::metadata::core::Spec;
    use crate::metadata::store::LocalStore;
    use crate::metadata::store::DualEpochMap;
    use crate::metadata::store::MetadataStoreObject;
    use crate::metadata::spu::SpuSpec;
    use crate::metadata::core::MetadataItem;

    pub(crate) type CacheMetadataStoreObject<S> = MetadataStoreObject<S, AlwaysNewContext>;

    /// Timeout
    static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
        use std::env;
        let var_value = env::var("FLV_METADATA_TIMEOUT").unwrap_or_default();
        let wait_time: u64 = var_value.parse().unwrap_or(60000); // up to 60 seconds
        wait_time
    });

    /// context that always updates
    #[derive(Debug, Default, Clone, Eq, PartialEq)]
    pub struct AlwaysNewContext {}

    impl MetadataItem for AlwaysNewContext {
        type UId = u64;

        fn uid(&self) -> &Self::UId {
            &0
        }

        /// always return true, this should be changed
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
    }

    impl<S> StoreContext<S>
    where
        S: Spec,
    {
        pub fn new() -> Self {
            Self {
                store: LocalStore::new_shared(),
            }
        }

        pub fn store(&self) -> &Arc<LocalStore<S, AlwaysNewContext>> {
            &self.store
        }

        /// L by key if doesn't exist return None
        /// This will generate timeout if metadata has not been filled
        #[instrument(
            skip(self),
            fields(
                Store = %S::LABEL            )
        )]
        pub async fn lookup_by_key(
            &self,
            key: &S::IndexKey,
        ) -> Result<Option<CacheMetadataStoreObject<S>>, IoError>
        where
            S: 'static,
            S::IndexKey: Display,
        {
            self.lookup_and_wait(|g| g.get(key).map(|v| v.inner().clone()))
                .await
        }

        #[instrument(skip(self, search))]
        async fn lookup_and_wait<'a, F>(
            &'a self,
            search: F,
        ) -> Result<Option<CacheMetadataStoreObject<S>>, IoError>
        where
            S: 'static,
            S::IndexKey: Display,
            F: Fn(
                RwLockReadGuard<'a, DualEpochMap<S::IndexKey, CacheMetadataStoreObject<S>>>,
            ) -> Option<CacheMetadataStoreObject<S>>,
        {
            use std::time::Duration;
            use std::io::ErrorKind;

            use tokio::select;
            use fluvio_future::timer::sleep;

            // We can short circuit here if already present
            if let Some(found) = search(self.store().read().await) {
                return Ok(Some(found));
            }

            let mut timer = sleep(Duration::from_millis(*MAX_WAIT_TIME));

            // No changes recieved yet, wait for first changes from store or timeout
            select! {

                _ = self.store.wait_for_first_change() => {
                    Ok(search(self.store().read().await))
                },
                _ = &mut timer => {
                    debug!(
                        SPEC = S::LABEL,
                        Timeout = *MAX_WAIT_TIME,
                        "store look up timeout expired");
                    Err(IoError::new(
                        ErrorKind::TimedOut,
                        format!("timed out searching metadata {} failed due to timeout: {} ms",S::LABEL,*MAX_WAIT_TIME),
                    ))
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
            .await?
            .ok_or(FluvioError::SPUNotFound(id))
        }
    }

    #[cfg(feature = "unstable")]
    mod unstable {
        use super::*;
        use crate::metadata::store::MetadataChanges;
        use futures_util::Stream;

        impl<S> StoreContext<S>
        where
            S: Spec + Send + Sync + 'static,
            <S as Spec>::Status: Send + Sync,
            S::IndexKey: Send + Sync,
        {
            pub fn watch(&self) -> impl Stream<Item = MetadataChanges<S, AlwaysNewContext>> {
                let mut listener = self.store.change_listener();
                let (sender, receiver) = async_channel::unbounded();

                fluvio_future::task::spawn_local(async move {
                    loop {
                        listener.listen().await;
                        let changes = listener.sync_changes().await;
                        if let Err(e) = sender.send(changes).await {
                            tracing::error!("Failed to send Metadata update: {:?}", e);
                            break;
                        }
                    }
                });

                receiver
            }
        }
    }
}
