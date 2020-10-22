use std::{collections::HashMap, path::Path};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::os::unix::io::AsRawFd;
use log::debug;
use x509_parser::parse_x509_der;
use async_trait::async_trait;

use fluvio_future::{net::TcpStream, tls::DefaultServerTlsStream};
use fluvio_protocol::api::{Request, RequestMessage, ResponseMessage};
use fluvio_protocol::derive::{Decode, Encode};
use flv_tls_proxy::authenticator::Authenticator;

#[derive(Decode, Encode, Debug, Default)]
pub struct AuthorizationRequest {
    principal: String,
    role: String,
}

#[derive(Decode, Encode, Debug, Default)]
pub struct AuthorizationResponse {
    success: bool,
}

impl Request for AuthorizationRequest {
    const API_KEY: u16 = 0;
    type Response = AuthorizationResponse;
}

struct RoleBindings(HashMap<String, String>);

impl RoleBindings {
    pub fn load(role_binding_file_path: &Path) -> Result<Self, IoError> {
        let file = std::fs::read_to_string(role_binding_file_path)?;
        Ok(Self(serde_json::from_str(&file)?))
    }
    pub fn get_role(&self, principal: &str) -> String {
        self.0[principal].clone()
    }
}

pub struct DefaultAuthenticator {
    role_bindings: RoleBindings,
}

impl DefaultAuthenticator {
    pub fn new(role_binding_file_path: &Path) -> Self {
        Self {
            role_bindings: RoleBindings::load(role_binding_file_path)
                .expect("unable to create RoleBindings"),
        }
    }

    async fn send_authorization_request(
        tcp_stream: &TcpStream,
        authorization_request: AuthorizationRequest,
    ) -> Result<bool, IoError> {
        let mut socket =
            fluvio_socket::FlvSocket::from_stream(tcp_stream.clone(), tcp_stream.as_raw_fd());

        let request_message = RequestMessage::new_request(authorization_request);

        let ResponseMessage { response, .. } =
            socket
                .send(&request_message)
                .await
                .map_err(|err| match err {
                    fluvio_socket::FlvSocketError::IoError { source } => source,
                    fluvio_socket::FlvSocketError::SendFileError { .. } => {
                        panic!("shoud not be doing zero copy here")
                    }
                })?;

        Ok(response.success)
    }

    fn principal_from_tls_stream(tls_stream: &DefaultServerTlsStream) -> Result<String, IoError> {
        let client_certificates = tls_stream
            .client_certificates()
            .ok_or(IoErrorKind::NotFound)?;
        let principal = client_certificates
            .iter()
            .filter_map(|cert| parse_x509_der(cert.as_ref()).ok())
            .map(|cert| cert.1)
            .next()
            .ok_or(IoErrorKind::NotFound)?
            .subject()
            .iter_common_name()
            .filter_map(|cn_atv| cn_atv.as_str().ok())
            .map(|cn_str| cn_str.to_owned())
            .next()
            .ok_or(IoErrorKind::NotFound)?;

        debug!("decoded principal from cert: {:?}", principal);

        Ok(principal)
    }
}

#[async_trait]
impl Authenticator for DefaultAuthenticator {
    async fn authenticate(
        &self,
        incoming_tls_stream: &DefaultServerTlsStream,
        target_tcp_stream: &TcpStream,
    ) -> Result<bool, IoError> {
        let principal = Self::principal_from_tls_stream(incoming_tls_stream)?;
        let role = self.role_bindings.get_role(&principal);
        let authorization_request = AuthorizationRequest { principal, role };
        let success =
            Self::send_authorization_request(&target_tcp_stream, authorization_request).await?;
        Ok(success)
    }
}
