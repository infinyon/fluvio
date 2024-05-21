pub use common::*;

mod common {

    use std::sync::Arc;
    use std::fmt::Debug;

    use fluvio_stream_model::core::MetadataItem;

    use crate::core::SharedContext;

    /// SC global context with authorization
    /// auth is trait object which contains global auth auth policy
    #[derive(Clone, Debug)]
    pub struct ScAuthGlobalContext<A, C: MetadataItem> {
        pub global_ctx: SharedContext<C>,
        pub auth: Arc<A>,
    }

    impl<A, C> ScAuthGlobalContext<A, C>
    where
        C: MetadataItem,
    {
        pub fn new(global_ctx: SharedContext<C>, auth: Arc<A>) -> Self {
            Self { global_ctx, auth }
        }
    }

    /// Auth Service Context, this hold individual context that is enough enforce auth
    /// for this service context
    #[derive(Debug, Clone)]
    pub struct ScAuthServiceContext<AC, C: MetadataItem> {
        pub global_ctx: SharedContext<C>,
        pub auth: AC,
    }

    impl<AC, C> ScAuthServiceContext<AC, C>
    where
        C: MetadataItem,
    {
        pub fn new(global_ctx: SharedContext<C>, auth: AC) -> Self {
            Self { global_ctx, auth }
        }
    }
}
