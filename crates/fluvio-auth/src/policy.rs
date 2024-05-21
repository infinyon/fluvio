use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_socket::FluvioSocket;

use super::AuthError;

#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub enum TypeAction {
    Create,
    Read,
}

pub enum InstanceAction {
    Delete,
    Update,
}

#[async_trait]
pub trait AuthContext: Debug + Send + Sync + 'static {
    /// check if any allow type specific action can be allowed
    async fn allow_type_action(
        &self,
        ty: ObjectType,
        action: TypeAction,
    ) -> Result<bool, AuthError>;

    /// check if specific instance of action can be permitted
    async fn allow_instance_action(
        &self,
        ty: ObjectType,
        action: InstanceAction,
        key: &str,
    ) -> Result<bool, AuthError>;
}

#[async_trait]
pub trait Authorization {
    type Context: AuthContext;

    /// create auth context
    async fn create_auth_context(
        &self,
        socket: &mut FluvioSocket,
    ) -> Result<Self::Context, AuthError>;
}
