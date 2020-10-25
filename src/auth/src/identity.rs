use std::fmt::Debug;

use futures_util::io::{AsyncRead, AsyncWrite};
use async_trait::async_trait;
//use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};


use fluvio_auth_schema::AuthorizationScopes;
//use fluvio_service::{SocketBuilder};
use fluvio_socket::InnerFlvSocket;
//use fluvio_protocol::api::{ResponseMessage};
// use fluvio_auth_schema::{AuthorizationApiRequest, AuthResponse};
use fluvio_controlplane_metadata::core::Spec;



#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub enum TypeAction {
    Create,
    Read,
}

pub enum InstanceAction {
    Delete
}

/*
/// Enumerate objec type, can't use spec becuase we can't do generic in the trait object
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub enum ObjectType {
    Spu,
    CustomSpu,
    SpuGroup,
    Topic,
    Partition,
}
*/



#[async_trait]
pub trait AuthContext {

    /// check if any allow type specific action
    async fn type_action_allowed<S: Spec>(&self,action: TypeAction) -> Result<bool,std::io::Error>;

    /// check if specific instance of spec can be deleted
    async fn instance_action_allowed<S>(&self, action: InstanceAction, key: &S::IndexKey) -> Result<bool,std::io::Error>
        where S: Spec + Send,
             S::IndexKey: Sync;
    
}



#[async_trait]
pub trait Authorization
{

    type Stream: AsyncRead + AsyncWrite + Unpin + Send;
    type Context: AuthContext;

    /// create auth context
    async fn create_auth_context(&self, socket: &mut InnerFlvSocket<Self::Stream>
    ) -> Result<Self::Context, std::io::Error>;
        
}




#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthorizationIdentity {
    pub principal: String,
    pub scopes: AuthorizationScopes,
}

impl AuthorizationIdentity {
    pub fn scopes(&self) -> &AuthorizationScopes {
        &self.scopes
    }
}

/*
#[async_trait]
impl IdentityContext for AuthorizationIdentity {
    async fn create_from_connection<S>(
        socket: &mut InnerFlvSocket<<S>::Stream>,
    ) -> Result<Self, std::io::Error>
    where
        S: SocketBuilder,
    {
        let identity = {
            let stream = &mut socket.get_mut_stream();

            let mut api_stream = stream.api_stream::<AuthorizationApiRequest, _>();

            if let Some(msg) = api_stream.next().await {
                match msg {
                    Ok(req_msg) => match req_msg {
                        AuthorizationApiRequest::AuthRequest(req_msg) => AuthorizationIdentity {
                            scopes: req_msg.request.scopes,
                            principal: req_msg.request.principal,
                        },
                    },
                    Err(_e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "connection closed",
                        ))
                    }
                }
            } else {
                tracing::trace!("client connect terminated");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "connection closed",
                ));
            }
        };

        let sink = &mut socket.get_mut_sink();

        let response = AuthResponse { success: true };

        let msg = ResponseMessage {
            correlation_id: 0,
            response,
        };

        let version = 1;

        if let Ok(()) = sink.send_response(&msg, version).await {
            Ok(identity)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "connection interrupted during response",
            ))
        }
    }
}
*/