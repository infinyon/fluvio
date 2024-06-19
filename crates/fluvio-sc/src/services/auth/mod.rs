pub mod basic;

pub use common::*;

mod common {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;

    use fluvio_auth::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
    use fluvio_socket::FluvioSocket;
    use fluvio_controlplane_metadata::extended::ObjectType;
    use fluvio_stream_model::core::MetadataItem;

    use crate::core::SharedContext;

    /// SC global context with authorization
    /// auth is trait object which contains global auth auth policy
    #[derive(Clone, Debug)]
    pub struct AuthGlobalContext<A, C: MetadataItem> {
        pub global_ctx: SharedContext<C>,
        pub auth: Arc<A>,
    }

    impl<A, C> AuthGlobalContext<A, C>
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
    pub struct AuthServiceContext<AC, C: MetadataItem> {
        pub global_ctx: SharedContext<C>,
        pub auth: AC,
    }

    impl<AC, C> AuthServiceContext<AC, C>
    where
        C: MetadataItem,
    {
        pub fn new(global_ctx: SharedContext<C>, auth: AC) -> Self {
            Self { global_ctx, auth }
        }
    }

    /// Authorization that allows only read only ops
    #[derive(Debug, Clone)]
    pub struct ReadOnlyAuthorization {}

    #[async_trait]
    impl Authorization for ReadOnlyAuthorization {
        type Context = ReadOnlyAuthContext;

        async fn create_auth_context(
            &self,
            _socket: &mut FluvioSocket,
        ) -> Result<Self::Context, AuthError> {
            Ok(ReadOnlyAuthContext {})
        }
    }

    impl ReadOnlyAuthorization {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[derive(Debug)]
    pub struct ReadOnlyAuthContext {}

    #[async_trait]
    impl AuthContext for ReadOnlyAuthContext {
        async fn allow_type_action(
            &self,
            ty: ObjectType,
            action: TypeAction,
        ) -> Result<bool, AuthError> {
            Ok(matches!(action, TypeAction::Read) || matches!(ty, ObjectType::CustomSpu))
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

    #[cfg(test)]
    mod test {
        use fluvio_auth::{root::RootAuthContext, AuthContext};

        use super::{ObjectType, ReadOnlyAuthContext, TypeAction};

        /// test read only context
        /// read only context allows read on everything
        /// and create on spu
        #[fluvio_future::test]
        async fn test_read_only_context() {
            let auth_context = ReadOnlyAuthContext {};
            assert!(auth_context
                .allow_type_action(ObjectType::Spu, TypeAction::Read)
                .await
                .unwrap());
            assert!(!auth_context
                .allow_type_action(ObjectType::Spu, TypeAction::Create)
                .await
                .unwrap());
            assert!(auth_context
                .allow_type_action(ObjectType::Topic, TypeAction::Read)
                .await
                .unwrap());
            assert!(!auth_context
                .allow_type_action(ObjectType::Topic, TypeAction::Create)
                .await
                .unwrap());
        }

        /// test root context
        /// root context allows everything
        #[fluvio_future::test]
        async fn test_root_context() {
            let auth_context = RootAuthContext {};
            assert!(auth_context
                .allow_type_action(ObjectType::Spu, TypeAction::Read)
                .await
                .unwrap());
            assert!(auth_context
                .allow_type_action(ObjectType::Spu, TypeAction::Create)
                .await
                .unwrap());
            assert!(auth_context
                .allow_type_action(ObjectType::Topic, TypeAction::Read)
                .await
                .unwrap());
            assert!(auth_context
                .allow_type_action(ObjectType::Topic, TypeAction::Create)
                .await
                .unwrap());
        }
    }
}
