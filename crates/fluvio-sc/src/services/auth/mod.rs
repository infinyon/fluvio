pub mod basic;

pub use common::*;

mod common {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;

    use fluvio_auth::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
    use fluvio_socket::FluvioSocket;
    use fluvio_controlplane_metadata::extended::ObjectType;

    use crate::core::SharedContext;

    /// SC global context with authorization
    /// auth is trait object which contains global auth auth policy
    #[derive(Clone, Debug)]
    pub struct AuthGlobalContext<A> {
        pub global_ctx: SharedContext,
        pub auth: Arc<A>,
    }

    impl<A> AuthGlobalContext<A> {
        pub fn new(global_ctx: SharedContext, auth: Arc<A>) -> Self {
            Self { global_ctx, auth }
        }
    }

    /// Authorization that allows anything
    /// Used for personal development
    #[derive(Debug, Clone)]
    pub struct RootAuthorization {}

    #[async_trait]
    impl Authorization for RootAuthorization {
        type Context = RootAuthContext;

        async fn create_auth_context(
            &self,
            _socket: &mut FluvioSocket,
        ) -> Result<Self::Context, AuthError> {
            Ok(RootAuthContext {})
        }
    }

    impl RootAuthorization {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[derive(Debug)]
    pub struct RootAuthContext {}

    #[async_trait]
    impl AuthContext for RootAuthContext {
        async fn allow_type_action(
            &self,
            _ty: ObjectType,
            _action: TypeAction,
        ) -> Result<bool, AuthError> {
            Ok(true)
        }

        /// check if specific instance of spec can be deleted
        async fn allow_instance_action(
            &self,
            _ty: ObjectType,
            _action: InstanceAction,
            _key: &str,
        ) -> Result<bool, AuthError> {
            Ok(true)
        }
    }

    /// Auth Service Context, this hold individual context that is enough enforce auth
    /// for this service context
    #[derive(Debug, Clone)]
    pub struct AuthServiceContext<AC> {
        pub global_ctx: SharedContext,
        pub auth: AC,
    }

    impl<AC> AuthServiceContext<AC> {
        pub fn new(global_ctx: SharedContext, auth: AC) -> Self {
            Self { global_ctx, auth }
        }
    }
}
