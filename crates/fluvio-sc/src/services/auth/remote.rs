use tracing::instrument;
use async_trait::async_trait;

use fluvio_auth::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_auth::x509::X509Identity;

#[derive(Debug, Clone)]
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

    /// check if specific instance of spec can be deleted
    async fn allow_instance_action(
        &self,
        _ty: ObjectType,
        _action: InstanceAction,
        _key: &str,
    ) -> Result<bool, AuthError> {
        Ok(true)
    }

    // check if remote id is allowed
    fn allow_remote_id(&self, id: &str) -> bool {
        self.identity.principal == id
    }
}
