use async_trait::async_trait;
use fluvio_auth::{
    x509::X509Identity, AuthContext, AuthError, Authorization, InstanceAction, TypeAction,
};
use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_socket::FluvioSocket;
use tracing::{info, instrument};

/// Authorization that allows by remote Id
#[derive(Debug, Clone)]
pub struct SpuRemoteAuthorization {}

#[async_trait]
impl Authorization for SpuRemoteAuthorization {
    type Context = SpuRemoteAuthContext;

    #[instrument(level = "trace", skip(self, socket))]
    async fn create_auth_context(
        &self,
        socket: &mut FluvioSocket,
    ) -> Result<Self::Context, AuthError> {
        info!("remote_cert DEBUG_1");

        let identity = X509Identity::create_from_connection(socket)
            .await
            .map_err(|err| {
                tracing::error!(%err, "failed to create x509 identity");
                err
            })?;

        info!("remote_cert DEBUG_2");
        Ok(SpuRemoteAuthContext { identity })
    }
}

impl SpuRemoteAuthorization {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct SpuRemoteAuthContext {
    identity: X509Identity,
}

#[async_trait]
impl AuthContext for SpuRemoteAuthContext {
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

    fn allow_remote_id(&self, id: &str) -> bool {
        self.identity.principal == id
    }
}

#[cfg(test)]
mod test {
    use fluvio_auth::AuthContext;

    use super::{ObjectType, SpuRemoteAuthContext, TypeAction};

    /// test remote context
    #[fluvio_future::test]
    async fn test_remote_context() {
        let auth_context = SpuRemoteAuthContext {
            identity: Default::default(),
        };
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
