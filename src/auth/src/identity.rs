
use async_trait::async_trait;
//use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};


use fluvio_auth_schema::AuthorizationScopes;
//use fluvio_service::{SocketBuilder};
use fluvio_socket::InnerFlvSocket;
//use fluvio_protocol::api::{ResponseMessage};
// use fluvio_auth_schema::{AuthorizationApiRequest, AuthResponse};
use fluvio_controlplane_metadata::core::Spec;

#[async_trait]
pub trait AuthContext {

    /// check if allows to create any instance of spec
    async fn create<S: Spec>(&self) -> Result<bool,std::io::Error> ;

    /// check if specific instance of spec can be deleted
    async fn delete<S: Spec>(&self,key: &S::IndexKey) -> Result<bool,std::io::Error>;

    /// check if any instance of spec can be reead;
    /// this is not per instance
    async fn read<S: Spec>(&self) -> Result<bool,std::io::Error>;
    
}



#[async_trait]
pub trait Authorization
{

    /// create auth context
    fn create_auth_context<C:AuthContext,S>(&self, socket: &mut InnerFlvSocket<S>
    ) -> Result<C, std::io::Error>;
        
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