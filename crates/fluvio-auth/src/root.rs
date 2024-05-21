use std::fmt::Debug;

use async_trait::async_trait;

use crate::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
use fluvio_socket::FluvioSocket;
use fluvio_controlplane_metadata::extended::ObjectType;

/// Authorization that allows anything
/// Used for personal development
#[derive(Debug, Clone, Default)]
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

/// Authorization that allows only read only ops
#[derive(Debug, Clone, Default)]
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
    use crate::AuthContext;

    use super::{ObjectType, ReadOnlyAuthContext, RootAuthContext, TypeAction};

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
