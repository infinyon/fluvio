use async_trait::async_trait;

use std::fs::read;
use std::collections::HashMap;
use std::path::PathBuf;
use std::convert::TryFrom;

use serde::{Serialize, Deserialize};

use fluvio_future::net::TcpStream; 
use fluvio_auth::{ AuthContext, Authorization, TypeAction, InstanceAction};
use fluvio_controlplane_metadata::core::Spec;
//use fluvio_service::{Authorization, AuthorizationError};

pub type Role = String;


pub struct BasicAuthorization {}

impl BasicAuthorization {

    pub fn load_from(config: String) -> Self {
        Self{}
    }
}

#[async_trait]
impl Authorization for BasicAuthorization {
    type Stream = TcpStream;
    type Context = BasicAuthContext;

    async fn create_auth_context(&self, socket: &mut fluvio_socket::InnerFlvSocket<Self::Stream>
    ) -> Result<Self::Context, std::io::Error> {
        Ok(BasicAuthContext{})
    }
}

pub struct BasicAuthContext {

}

#[async_trait]
impl AuthContext for BasicAuthContext {

    
    async fn type_action_allowed<S: Spec>(&self,action: TypeAction) -> Result<bool,std::io::Error> {
        Ok(true)
    }

    /// check if specific instance of spec can be deleted
    async fn instance_action_allowed<S: Spec + Send >(&self, action: InstanceAction, key: &S::IndexKey) -> Result<bool,std::io::Error> {
        Ok(true)
    }


}

/*
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
*/