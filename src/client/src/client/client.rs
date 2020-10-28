use std::default::Default;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use tracing::trace;
use async_trait::async_trait;

use dataplane::api::RequestMessage;
use dataplane::api::Request;
use fluvio_spu_schema::server::versions::{ApiVersions, ApiVersionsRequest};
use fluvio_socket::FlvSocketError;
use fluvio_socket::{AllFlvSocket, AllSerialSocket};

#[cfg(feature = "rust_tls")]
use fluvio_future::tls::AllDomainConnector;

#[cfg(feature = "native_tls")]
use fluvio_future::native_tls::AllDomainConnector;

use crate::FluvioError;

/// Frame with request and response
#[async_trait]
pub(crate) trait SerialFrame: Sync + Send + Display {
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
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, FlvSocketError>
    where
        R: Request + Send + Sync;
}

/// This sockets knows about support versions
/// Version information are automatically  insert into request
pub struct VersionedSocket {
    socket: AllFlvSocket,
    config: ClientConfig,
    versions: Versions,
}

impl fmt::Display for VersionedSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "config {}", self.config)
    }
}

#[async_trait]
impl SerialFrame for VersionedSocket {
    fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// send and wait for reply
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, FlvSocketError>
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

impl VersionedSocket {
    /// connect to end point and retrieve versions
    pub async fn connect(
        mut socket: AllFlvSocket,
        config: ClientConfig,
    ) -> Result<Self, FluvioError> {
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

    pub fn split(self) -> (AllFlvSocket, ClientConfig, Versions) {
        (self.socket, self.config, self.versions)
    }

    /// send request only
    pub async fn send_request<R>(&mut self, request: R) -> Result<RequestMessage<R>, FlvSocketError>
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

/// Connection Config to any client
#[derive(Clone)]
pub struct ClientConfig {
    addr: String,
    client_id: String,
    connector: Arc<AllDomainConnector>,
}

impl fmt::Display for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "addr {}", self.addr)
    }
}

impl From<String> for ClientConfig {
    fn from(addr: String) -> Self {
        Self::with_addr(addr)
    }
}

impl ClientConfig {
    pub fn new<S: Into<String>>(addr: S, connector: Arc<AllDomainConnector>) -> Self {
        Self {
            addr: addr.into(),
            client_id: "fluvio".to_owned(),
            connector,
        }
    }

    pub fn with_addr(addr: String) -> Self {
        Self::new(addr, Arc::new(AllDomainConnector::default()))
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// set client id
    #[allow(unused)]
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

    pub(crate) async fn connect(self) -> Result<VersionedSocket, FluvioError> {
        let socket = AllFlvSocket::connect_with_connector(&self.addr, &*self.connector).await?;
        VersionedSocket::connect(socket, self).await
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

/// Connection that perform request/response
pub struct VersionedSerialSocket {
    socket: AllSerialSocket,
    config: ClientConfig,
    versions: Versions,
}

impl fmt::Display for VersionedSerialSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "config {}", self.config)
    }
}

impl VersionedSerialSocket {
    pub fn new(socket: AllSerialSocket, config: ClientConfig, versions: Versions) -> Self {
        Self {
            socket,
            config,
            versions,
        }
    }
}

#[async_trait]
impl SerialFrame for VersionedSerialSocket {
    fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// send and wait for reply serially
    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, FlvSocketError>
    where
        R: Request + Send + Sync,
    {
        let req_msg = self.new_request(request, self.versions.lookup_version(R::API_KEY));

        // send request & save response
        self.socket.send_and_receive(req_msg).await
    }
}
