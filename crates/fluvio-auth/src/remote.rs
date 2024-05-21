use tracing::instrument;
use async_trait::async_trait;

use crate::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
use fluvio_controlplane_metadata::extended::ObjectType;
use crate::x509::X509Identity;

#[derive(Debug, Clone, Default)]
pub struct RemoteAuthorization {}

impl RemoteAuthorization {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Authorization for RemoteAuthorization {
    type Context = RemoteAuthContext;

    #[instrument(level = "trace", skip(self, socket))]
    async fn create_auth_context(
        &self,
        socket: &mut fluvio_socket::FluvioSocket,
    ) -> Result<Self::Context, AuthError> {
        let identity = X509Identity::create_from_connection(socket)
            .await
            .map_err(|err| {
                tracing::error!(%err, "failed to create x509 identity");
                err
            })?;
        Ok(RemoteAuthContext { identity })
    }
}

#[derive(Debug)]
pub struct RemoteAuthContext {
    identity: X509Identity,
}

#[async_trait]
impl AuthContext for RemoteAuthContext {
    async fn allow_type_action(
        &self,
        _ty: ObjectType,
        _action: TypeAction,
    ) -> Result<bool, AuthError> {
        Ok(true)
    }

    async fn allow_instance_action(
        &self,
        ty: ObjectType,
        _action: InstanceAction,
        key: &str,
    ) -> Result<bool, AuthError> {
        if ty == ObjectType::RemoteConnection {
            return Ok(self.identity.principal == key);
        }

        Ok(true)
    }
}
