use async_trait::async_trait;

use std::fs::read;
use std::collections::HashMap;
use std::path::PathBuf;
use std::convert::TryFrom;

use serde::{Serialize, Deserialize};

use fluvio_auth::identity::AuthorizationIdentity;
use fluvio_service::auth::{Authorization, AuthorizationError};

pub type Role = String;

#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub enum Action {
    Create,
    Read,
    Update,
    Delete,
    All,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub enum Object {
    Spu,
    CustomSpu,
    SpuGroup,
    Topic,
    Partition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Policy(pub HashMap<Role, HashMap<Object, Vec<Action>>>);

impl From<HashMap<Role, HashMap<Object, Vec<Action>>>> for Policy {
    fn from(map: HashMap<Role, HashMap<Object, Vec<Action>>>) -> Self {
        Self(map)
    }
}

impl TryFrom<PathBuf> for Policy {
    type Error = std::io::Error;
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let file = read(path)?;
        let policy: Policy = serde_json::from_slice(&file)?;
        Ok(policy)
    }
}

impl Policy {
    pub async fn evaluate(
        &self,
        request: ScAuthorizationContextRequest,
        identity: &AuthorizationIdentity,
    ) -> Result<bool, AuthorizationError> {
        // For each scope provided in the identity,
        // check if there is a match;
        let is_allowed = identity.scopes().iter().any(|scope| {
            self.0
                .get(&scope.to_lowercase())
                .map(|objects| {
                    objects
                        .get(&request.1)
                        .map(|actions| {
                            actions
                                .iter()
                                .any(|action| action == &request.0 || action == &Action::All)
                        })
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        });

        Ok(is_allowed)
    }
}

impl Default for Policy {
    // default only allows the `Root` role to have full permissions;
    fn default() -> Self {
        let mut root_policy = HashMap::new();

        root_policy.insert(Object::Spu, vec![Action::All]);
        root_policy.insert(Object::CustomSpu, vec![Action::All]);
        root_policy.insert(Object::SpuGroup, vec![Action::All]);
        root_policy.insert(Object::Topic, vec![Action::All]);
        root_policy.insert(Object::Partition, vec![Action::All]);

        let mut policy = HashMap::new();

        policy.insert(String::from("Root"), root_policy);

        Self(policy)
    }
}

pub type ObjectName = String;

pub struct ScAuthorizationContext {
    pub identity: AuthorizationIdentity,
    pub policy: Policy,
}

pub type ScAuthorizationContextRequest = (Action, Object, Option<ObjectName>);

#[async_trait]
impl Authorization<Policy, AuthorizationIdentity> for ScAuthorizationContext {
    type Request = ScAuthorizationContextRequest;
    fn create_authorization_context(identity: AuthorizationIdentity, config: Policy) -> Self {
        ScAuthorizationContext {
            identity,
            policy: config,
        }
    }

    async fn enforce(&self, request: Self::Request) -> Result<bool, AuthorizationError> {
        Ok(self.policy.evaluate(request, &self.identity).await?)
    }
}
