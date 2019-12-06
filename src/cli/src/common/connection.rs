//!
//! # Send Request to Kafka server
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::{trace, debug};
use utils::generators::rand_correlation_id;

use kf_socket::KfSocket;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::Request;

use crate::error::CliError;

// -----------------------------------
// Structure
// -----------------------------------

#[derive(Debug)]
pub struct Connection {
    server_addr: SocketAddr,
    socket: KfSocket,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl Connection {
    /// Create a new connection
    pub async fn new(server_addr: &SocketAddr) -> Result<Self, CliError> {
        let socket = KfSocket::connect(&server_addr)
            .await
            .map_err(|err| IoError::new(ErrorKind::ConnectionRefused, format!("{}", err)))?;

        debug!("connected to: {}", server_addr);
        Ok(Connection {
            socket,
            server_addr: server_addr.clone(),
        })
    }

    /// Send request and return response (or error)
    pub async fn send_request<R>(
        &mut self,
        request: R,
        version: Option<i16>,
    ) -> Result<R::Response, CliError>
    where
        R: Request,
    {
        trace!("send API '{}' req to srv '{}'", R::API_KEY, self.server_addr);

        let mut req_msg: RequestMessage<R> = RequestMessage::new_request(request);
        req_msg
            .get_mut_header()
            .set_client_id("fluvio")
            .set_correlation_id(rand_correlation_id());
        if let Some(ver) = version {
            req_msg.get_mut_header().set_api_version(ver);
        }
        // send request & save response
        match self.socket.send(&req_msg).await {
            Err(err) => Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!(
                    "rsvd '{}' from srv '{}': {}",
                    R::API_KEY,
                    self.server_addr,
                    err
                ),
            ))),
            Ok(response) => {
                trace!("rsvd '{}' res from srv '{}' ", R::API_KEY, self.server_addr);
                Ok(response.response)
            }
        }
    }

    /// Accessor for server address
    pub fn server_addr(&self) -> &SocketAddr {
        &self.server_addr
    }
}
