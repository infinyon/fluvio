use std::{collections::HashMap, path::Path};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use fluvio_future::net::AsConnectionFd;

use tracing::{debug, trace};
use x509_parser::{certificate::X509Certificate, parse_x509_certificate};
use async_trait::async_trait;

use fluvio_future::{net::TcpStream, openssl::DefaultServerTlsStream};
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use flv_tls_proxy::authenticator::Authenticator;

use super::request::{AuthRequest};

#[derive(Debug)]
struct ScopeBindings(HashMap<String, Vec<String>>);

impl ScopeBindings {
    pub fn load(scope_binding_file_path: &Path) -> Result<Self, IoError> {
        let file = std::fs::read_to_string(scope_binding_file_path)?;
        let scope_bindings = Self(serde_json::from_str(&file)?);
        debug!("scope bindings loaded {:?}", scope_bindings);
        Ok(scope_bindings)
    }
    pub fn get_scopes(&self, principal: &str) -> Vec<String> {
        trace!("getting scopes for principal {:?}", principal);
        if let Some(scopes) = self.0.get(principal) {
            trace!("scopes found for principal {:?}: {:?}", principal, scopes);
            scopes.clone()
        } else {
            trace!("scopes not found for principal {:?}", principal);
            Vec::new()
        }
    }
}

#[derive(Debug)]
pub struct X509Authenticator {
    scope_bindings: ScopeBindings,
}

impl X509Authenticator {
    pub fn new(scope_binding_file_path: &Path) -> Self {
        Self {
            scope_bindings: ScopeBindings::load(scope_binding_file_path)
                .expect("unable to create ScopeBindings"),
        }
    }

    async fn send_authorization_request(
        tcp_stream: &TcpStream,
        authorization_request: AuthRequest,
    ) -> Result<bool, IoError> {
        let fd = tcp_stream.as_connection_fd();

        let mut socket = fluvio_socket::FluvioSocket::from_stream(
            Box::new(tcp_stream.clone()),
            Box::new(tcp_stream.clone()),
            fd,
        );

        let request_message = RequestMessage::new_request(authorization_request);

        let ResponseMessage { response, .. } =
            socket
                .send(&request_message)
                .await
                .map_err(|err| match err {
                    fluvio_socket::SocketError::Io { source, .. } => source,
                    fluvio_socket::SocketError::SocketClosed
                    | fluvio_socket::SocketError::SocketStale => {
                        IoError::new(IoErrorKind::BrokenPipe, "connection closed")
                    }
                })?;

        Ok(response.success)
    }

    fn principal_from_tls_stream(tls_stream: &DefaultServerTlsStream) -> Result<String, IoError> {
        trace!("tls_stream {:?}", tls_stream);

        let peer_certificate = tls_stream.peer_certificate();

        trace!("peer_certificate {:?}", peer_certificate);

        let client_certificate = tls_stream.peer_certificate().ok_or(IoErrorKind::NotFound)?;

        trace!("client_certificate {:?}", tls_stream);

        let principal = Self::principal_from_raw_certificate(
            &client_certificate
                .to_der()
                .map_err(|err| err.into_io_error())?,
        )?;

        Ok(principal)
    }

    fn principal_from_raw_certificate(certificate_bytes: &[u8]) -> Result<String, IoError> {
        parse_x509_certificate(certificate_bytes)
            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))
            .and_then(|(_, parsed_cert)| Self::common_name_from_parsed_certificate(parsed_cert))
    }

    fn common_name_from_parsed_certificate(
        certificate: X509Certificate,
    ) -> Result<String, IoError> {
        certificate
            .subject()
            .iter_common_name()
            .next()
            .ok_or_else(|| IoErrorKind::NotFound.into())
            .and_then(|cn_atv| {
                cn_atv
                    .as_str()
                    .map(|cn_str| {
                        let cn_string = cn_str.to_owned();
                        debug!("common_name from cert: {:?}", cn_string);
                        cn_string
                    })
                    .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))
            })
    }
}

#[async_trait]
impl Authenticator for X509Authenticator {
    async fn authenticate(
        &self,
        incoming_tls_stream: &DefaultServerTlsStream,
        target_tcp_stream: &TcpStream,
    ) -> Result<bool, IoError> {
        let principal = Self::principal_from_tls_stream(incoming_tls_stream)?;
        let scopes = self.scope_bindings.get_scopes(&principal);
        let authorization_request = AuthRequest::new(principal, scopes);
        let success =
            Self::send_authorization_request(target_tcp_stream, authorization_request).await?;
        Ok(success)
    }
}

#[cfg(test)]
mod tests {
    use super::X509Authenticator;

    #[test]
    fn test_principal_from_raw_certificate() {
        let (_, pem) = x509_parser::prelude::parse_x509_pem(TEST_CERTIFICATE.as_bytes()).unwrap();
        let common_name = X509Authenticator::principal_from_raw_certificate(&pem.contents).unwrap();
        assert_eq!(common_name, "root".to_owned());
    }

    const TEST_CERTIFICATE: &str = r#"-----BEGIN CERTIFICATE-----
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
9yRsdBnrZVYxKKafmgz9omKDVFUVEtd39oo=
-----END CERTIFICATE-----"#;
}
