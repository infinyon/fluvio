use async_trait::async_trait;
use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_socket::FluvioSocket;

use crate::{AuthContext, AuthError, Authorization, InstanceAction, TypeAction};

/// Authorization that allows anything
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
