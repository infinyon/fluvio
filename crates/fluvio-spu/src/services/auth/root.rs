use async_trait::async_trait;
use fluvio_auth::{AuthContext, AuthError, Authorization, InstanceAction, TypeAction};
use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_socket::FluvioSocket;
use tracing::instrument;

/// Authorization that allows by remote Id
#[derive(Debug, Clone)]
pub struct SpuRootAuthorization {}

#[async_trait]
impl Authorization for SpuRootAuthorization {
    type Context = SpuRootAuthContext;

    #[instrument(level = "trace", skip(self, _socket))]
    async fn create_auth_context(
        &self,
        _socket: &mut FluvioSocket,
    ) -> Result<Self::Context, AuthError> {
        Ok(SpuRootAuthContext {})
    }
}

impl SpuRootAuthorization {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct SpuRootAuthContext {}

#[async_trait]
impl AuthContext for SpuRootAuthContext {
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

    fn allow_remote_id(&self, _id: &str) -> bool {
        true
    }
}
