pub use common::*;

mod common {

    use std::sync::Arc;
    use std::fmt::Debug;

    use crate::core::DefaultSharedGlobalContext;

    /// SPU global context with authorization
    /// auth is trait object which contains global auth auth policy
    #[derive(Clone, Debug)]
    pub struct SpuAuthGlobalContext<A> {
        pub global_ctx: DefaultSharedGlobalContext,
        pub auth: Arc<A>,
    }

    impl<A> SpuAuthGlobalContext<A> {
        pub fn new(global_ctx: DefaultSharedGlobalContext, auth: Arc<A>) -> Self {
            Self { global_ctx, auth }
        }
    }

    /// Auth Service Context, this hold individual context that is enough enforce auth
    /// for this service context
    #[derive(Debug, Clone)]
    pub struct SpuAuthServiceContext<AC> {
        pub global_ctx: DefaultSharedGlobalContext,
        pub auth: AC,
    }

    impl<AC> SpuAuthServiceContext<AC> {
        pub fn new(global_ctx: DefaultSharedGlobalContext, auth: AC) -> Self {
            Self { global_ctx, auth }
        }
    }
}
