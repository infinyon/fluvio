mod controller;
mod store;

pub use store::*;
pub use context::*;

mod context {

    use fluvio_stream_dispatcher::store::StoreContext as DispatcherStoreContext;
    use crate::metadata::core::MetadataItem;

    use crate::metadata::store::MetadataStoreObject;

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

    pub(crate) type CacheMetadataStoreObject<S> = MetadataStoreObject<S, AlwaysNewContext>;
    pub(crate) type StoreContext<S> = DispatcherStoreContext<S, AlwaysNewContext>;
}
