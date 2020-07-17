use std::default::Default;

use log::trace;
use async_trait::async_trait;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::Request;
use spu_api::server::versions::{ApiVersions, ApiVersionsRequest};
use kf_socket::*;
use flv_future_aio::net::tls::AllDomainConnector;

use crate::ClientError;


/// Generic client trait
#[async_trait]
pub trait Client: Sync + Send  {

    /// client config
    fn config(&self) -> &ClientConfig;

    /// create new request based on version
    fn new_request<R>(&self, request: R, version: Option<i16>) -> RequestMessage<R>
    where
        R: Request + Send,
    {
        let mut req_msg = RequestMessage::new_request(request);
        req_msg
            .get_mut_header()
            .set_client_id(&self.config().client_id);

        if let Some(ver) = version {
            req_msg.get_mut_header().set_api_version(ver);
        }
        req_msg
    }

    /// send and receive
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, KfSocketError>
        where R: Request + Send + Sync;

}

/// Client to fluvio component
///
pub struct RawClient {
    socket: AllKfSocket,
    config: ClientConfig,
    versions: Versions,
}

#[async_trait]
impl Client for RawClient {

    fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// send and wait for reply
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, KfSocketError>
    where
        R: Request + Send + Sync,
    {
        let req_message = self.send_request(request).await?;

        // send request & save response
        self.socket
            .get_mut_stream()
            .next_response(&req_message)
            .await
            .map(|res_msg| res_msg.response)
    }
}

impl RawClient {
    /// connect to end point and retrieve versions
    pub async fn connect(
        mut socket: AllKfSocket,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        // now get versions
        // Query for API versions
        let mut req_msg = RequestMessage::new_request(ApiVersionsRequest::default());
        req_msg.get_mut_header().set_client_id(&config.client_id);

        let response = (socket.send(&req_msg).await?).response;

        Ok(Self {
            socket,
            config,
            versions: Versions::new(response.api_keys),
        })
    }

    pub fn split(self) -> (AllKfSocket, ClientConfig, Versions) {
        (self.socket, self.config, self.versions)
    }

    

    /// send request only
    pub async fn send_request<R>(&mut self, request: R) -> Result<RequestMessage<R>, KfSocketError>
    where
        R: Request + Send + Sync,
    {
        trace!(
            "send API '{}' req to srv '{}'",
            R::API_KEY,
            self.config.addr()
        );

        let req_msg = self.new_request(request, self.versions.lookup_version(R::API_KEY));

        self.socket.get_mut_sink().send_request(&req_msg).await?;
        Ok(req_msg)
    }

    


}

/// Client Factory
#[derive(Clone)]
pub struct ClientConfig {
    addr: String,
    client_id: String,
    connector: AllDomainConnector,
}

impl From<String> for ClientConfig {
    fn from(addr: String) -> Self {
        Self::with_addr(addr)
    }
}

impl ClientConfig {
    pub fn new(addr: String, connector: AllDomainConnector) -> Self {
        Self {
            addr,
            client_id: "fluvio".to_owned(),
            connector,
        }
    }

    pub fn with_addr(addr: String) -> Self {
        Self::new(addr, AllDomainConnector::default())
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

    pub fn set_addr(&mut self, domain: String) {
        self.addr = domain
    }

    pub(crate) async fn connect(self) -> Result<RawClient, ClientError> {
        let socket = AllKfSocket::connect_with_connector(&self.addr, &self.connector).await?;
        RawClient::connect(socket, self).await
    }
}

/// wrap around versions
#[derive(Clone)]
pub struct Versions(ApiVersions);

impl Versions {

    pub fn new(versions: ApiVersions) -> Self {
        Self(versions)
    }


    /// Given an API key, it returns max_version. None if not found
    pub fn lookup_version(&self, api_key: u16) -> Option<i16> {
        for version in &self.0 {
            if version.api_key == api_key as i16 {
                return Some(version.max_version);
            }
        }
        None
    }

}

/// Client that performs serial request and response
/// This wraps Serial Multiplex Client
pub struct SerialClient {
    socket: AllSerialSocket,
    config: ClientConfig,
    versions: Versions
}

impl SerialClient {

    pub fn new(socket: AllSerialSocket,config: ClientConfig,versions: Versions) -> Self {
        Self {
            socket,
            config,
            versions
        }
    }



     

}

#[async_trait]
impl Client for SerialClient {

    fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// send and wait for reply serially
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, KfSocketError>
    where
        R: Request + Send + Sync
    {
        let req_msg = self.new_request(request, self.versions.lookup_version(R::API_KEY));
        
        // send request & save response
        self.socket.send_and_receive(req_msg).await
    }   

}



