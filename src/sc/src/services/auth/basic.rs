use async_trait::async_trait;




use fluvio_future::net::TcpStream; 
use fluvio_auth::{ AuthContext, Authorization, TypeAction, InstanceAction, AuthError };
use fluvio_controlplane_metadata::core::Spec;


#[derive(Debug,Clone)]
pub struct BasicAuthorization {}

impl BasicAuthorization {

    pub fn load_from(config: &str) -> Self {
        Self{}
    }
}

#[async_trait]
impl Authorization for BasicAuthorization {
    type Stream = TcpStream;
    type Context = BasicAuthContext;

    async fn create_auth_context(&self, socket: &mut fluvio_socket::InnerFlvSocket<Self::Stream>
    ) -> Result<Self::Context, AuthError> {
        Ok(BasicAuthContext{})
    }
}

#[derive(Debug)]
pub struct BasicAuthContext {

}

#[async_trait]
impl AuthContext for BasicAuthContext {

    
    async fn allow_type_action<S: Spec>(&self,action: TypeAction) -> Result<bool,AuthError> {
        Ok(true)
    }

    /// check if specific instance of spec can be deleted
    async fn allow_instance_action<S>(&self, action: InstanceAction, key: &S::IndexKey) -> Result<bool,AuthError>
        where S: Spec + Send,
            S::IndexKey: Sync
    {
        Ok(true)
    }


}

use policy::*;

/// basic policy module
/// does imple subtitution
mod policy {

    use std::fs::read;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::convert::TryFrom;

    use serde::{Serialize, Deserialize};

    use fluvio_auth::AuthError;
    use fluvio_auth::x509_identity::X509Identity;

    type Role = String;


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

    /// authorization request map to basic policy
    pub type BasicAuthorizationRequest = (Action, Object, Option<ObjectName>);


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
            request: BasicAuthorizationRequest,
            identity: &X509Identity,
        ) -> Result<bool, AuthError> {
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
}



/*
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


#[cfg(test)]
mod test {

    use std::fs::File;
    use std::path::PathBuf;
    use std::convert::TryFrom;
    use std::collections::HashMap;

    use fluvio_auth::x509_identity::X509Identity;
    use fluvio_future::test_async;
    
    use super::{ Action, Object, Policy};

    #[test]
    fn test_policy_serialization() {
        let mut policy = Policy::default();

        let mut default_role = HashMap::new();

        default_role.insert(Object::Topic, vec![Action::All]);
        default_role.insert(Object::Partition, vec![Action::All]);
        default_role.insert(Object::SpuGroup, vec![Action::Read]);
        default_role.insert(Object::CustomSpu, vec![Action::Read]);
        default_role.insert(Object::Spu, vec![Action::Read]);

        policy.0.insert(String::from("Default"), default_role);

        let tmp_file_path = PathBuf::from("/tmp/policy.json");
        let tmp = File::create(tmp_file_path.clone()).expect("failed to create policy file");
        serde_json::to_writer(&tmp, &policy).expect("failed to serialize policy to json file");

        let recovered_policy =
            Policy::try_from(tmp_file_path).expect("failed to parse policy from file");

        assert_eq!(
            policy, recovered_policy,
            "serialized and deserialized policies from file should match"
        )
    }

    #[test_async]
    async fn test_policy_enforcement_simple() -> Result<(),()> {

        let mut policy = Policy::default();
        let identity = X509Identity::new("User".to_owned(),vec!["Default".to_owned()]);
       
        let mut role1 = HashMap::new();
        role1.insert(Object::Topic, vec![Action::Delete,Action::Read]);

        policy.0.insert(String::from("Default"), role1);


        assert!(policy.evaluate((Action::Create,Object::CustomSpu,None),&identity).await.expect("eval"));

        Ok(())
    }

}