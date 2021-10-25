use serde::{Serialize, Deserialize};

use futures_util::stream::StreamExt;

use fluvio_protocol::api::{ResponseMessage};
use fluvio_socket::FluvioSocket;

use super::request::{AuthorizationScopes, AuthorizationApiRequest, AuthResponse};

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct X509Identity {
    pub principal: String,
    pub scopes: AuthorizationScopes,
}

impl X509Identity {
    pub fn new(principal: String, scopes: AuthorizationScopes) -> Self {
        Self { principal, scopes }
    }

    pub fn scopes(&self) -> &AuthorizationScopes {
        &self.scopes
    }

    /// extract x509 identity from TCP Socket
    pub async fn create_from_connection(socket: &mut FluvioSocket) -> Result<Self, std::io::Error> {
        let identity = {
            let stream = &mut socket.get_mut_stream();

            let mut api_stream = stream.api_stream::<AuthorizationApiRequest, _>();

            if let Some(msg) = api_stream.next().await {
                match msg {
                    Ok(req_msg) => match req_msg {
                        AuthorizationApiRequest::AuthRequest(req_msg) => Self {
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

        let msg = ResponseMessage::new(0, response);

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
