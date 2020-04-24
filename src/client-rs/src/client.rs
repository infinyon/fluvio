use std::default::Default;

use log::trace;
use rand::prelude::*;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::Request;
use spu_api::versions::{ApiVersions, ApiVersionsRequest};
use kf_socket::AllKfSocket;
use kf_socket::KfSocketError;
use flv_future_aio::net::tls::AllDomainConnector;

use crate::ClientError;


/// Generate a random correlation_id (0 to 65535)
fn rand_correlation_id() -> i32 {
    thread_rng().gen_range(0, 65535)
}

/// Client to fluvio component
/// 
pub struct Client {
    socket: AllKfSocket,
    config: ClientConfig,
    versions: ApiVersions,
}

impl Client {
    /// connect to established socket, retrieve version information
    pub async fn connect(
        mut socket: AllKfSocket,
        config: ClientConfig
    ) -> Result<Self, ClientError> {
        
        // now get versions
        // Query for API versions
        let mut req_msg = RequestMessage::new_request(ApiVersionsRequest::default());
        req_msg.get_mut_header().set_client_id(&config.client_id);

        let response = (socket.send(&req_msg).await?).response;

        Ok(Self {
            socket,
            config,
            versions: response.api_keys,
        })
    }

    pub fn new_request<R>(&self, request: R, version: Option<i16>) -> RequestMessage<R>
    where
        R: Request,
    {
        let mut req_msg = RequestMessage::new_request(request);
        req_msg
            .get_mut_header()
            .set_client_id(&self.config.client_id)
            .set_correlation_id(rand_correlation_id());
        if let Some(ver) = version {
            req_msg.get_mut_header().set_api_version(ver);
        }
        req_msg
    }

    /// Given an API key, it returns max_version. None if not found
    pub fn lookup_version(&self, api_key: u16) -> Option<i16> {
        for version in &self.versions {
            if version.api_key == api_key as i16 {
                return Some(version.max_version);
            }
        }
        None
    }

    pub fn addr(&self) -> &str {
        &self.config.addr
    }
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn socket(&self) -> &AllKfSocket {
        &self.socket
    }

    pub fn mut_socket(&mut self) -> &mut AllKfSocket {
        &mut self.socket
    }

    /// send request only
    pub async fn send_request<R>(&mut self, request: R) -> Result<RequestMessage<R>,KfSocketError>
    where
        R: Request,
    {
        trace!(
            "send API '{}' req to srv '{}'",
            R::API_KEY,
            self.config.addr()
        );

        let req_msg = self.new_request(request, self.lookup_version(R::API_KEY));

        self.socket.get_mut_sink().send_request(&req_msg)
            .await?;
        Ok(req_msg)
    }

    /// send and wait for reply
    pub async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, KfSocketError>
    where
        R: Request,
    {
        let req_message = self.send_request(request).await?;

        // send request & save response
        self.socket.get_mut_stream().next_response(&req_message).await
            .map(|res_msg| res_msg.response)
    }


    pub fn clone_config(&self) -> ClientConfig {
        self.config.clone()
    }
}


/// Client Factory
#[derive(Clone)]
pub struct ClientConfig{
    addr: String,
    client_id: String,
    connector: AllDomainConnector
}

impl From<String> for ClientConfig {

    fn from(addr: String) -> Self {
        Self::with_addr(addr)
    }
}

impl ClientConfig {
    pub fn new(addr: String,connector: AllDomainConnector) -> Self {
        Self {
            addr,
            client_id: "fluvio".to_owned(),
            connector
        }
    }

    pub fn with_addr(addr: String) -> Self {
        Self::new(addr,AllDomainConnector::default())
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// set client id
    pub fn set_client_id<S>(mut self, id: S) -> Self
    where
        S: Into<String>,
    {
        self.client_id = id.into();
        self
    }

    pub fn set_addr(&mut self,domain: String) {
        self.addr = domain
    }

    pub async fn connect(self) -> Result<Client, ClientError> 
    {
        let socket = AllKfSocket::connect_with_connector(&self.addr,&self.connector).await?;
        Client::connect(socket,self).await
    }
}

