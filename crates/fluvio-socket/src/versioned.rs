use std::default::Default;
use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use fluvio_protocol::Version;
use tracing::{debug, instrument, info};

use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::Request;
use fluvio_protocol::link::versions::{ApiVersions, ApiVersionsRequest, ApiVersionsResponse};
use fluvio_future::net::{DomainConnector, DefaultDomainConnector};
use fluvio_future::retry::retry_if;

use crate::{SocketError, FluvioSocket, SharedMultiplexerSocket, AsyncResponse};

/// Frame with request and response
pub trait SerialFrame: Display {
    /// client config
    fn config(&self) -> &ClientConfig;
}

/// This sockets knows about support versions
/// Version information are automatically  insert into request
pub struct VersionedSocket {
    socket: FluvioSocket,
    config: Arc<ClientConfig>,
    versions: Versions,
}

impl fmt::Display for VersionedSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "config {}", self.config)
    }
}

impl SerialFrame for VersionedSocket {
    fn config(&self) -> &ClientConfig {
        &self.config
    }
}

impl VersionedSocket {
    /// connect to end point and retrieve versions
    #[instrument(skip(socket, config))]
    pub async fn connect(
        mut socket: FluvioSocket,
        config: Arc<ClientConfig>,
    ) -> Result<Self, SocketError> {
        // now get versions
        // Query for API versions

        let version = ApiVersionsRequest {
            client_version: crate::built_info::PKG_VERSION.into(),
            client_os: crate::built_info::CFG_OS.into(),
            client_arch: crate::built_info::CFG_TARGET_ARCH.into(),
        };

        debug!(client_version = %version.client_version, "querying versions");
        let mut req_msg = RequestMessage::new_request(version);
        req_msg.get_mut_header().set_client_id(&config.client_id);

        let response: ApiVersionsResponse = (socket.send(&req_msg).await?).response;
        let versions = Versions::new(response);

        debug!("versions: {:#?}", versions);

        Ok(Self {
            socket,
            config,
            versions,
        })
    }

    pub fn split(self) -> (FluvioSocket, Arc<ClientConfig>, Versions) {
        (self.socket, self.config, self.versions)
    }
}

/// Low level configuration option to directly connect to Fluvio
/// This can bypass higher level validation required for CLI and end user application
pub struct ClientConfig {
    addr: String,
    client_id: String,
    connector: DomainConnector,
    use_spu_local_address: bool,
}

impl Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ClientConfig {{ addr: {}, client_id: {} }}",
            self.addr, self.client_id
        )
    }
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
    pub fn new(
        addr: impl Into<String>,
        connector: DomainConnector,
        use_spu_local_address: bool,
    ) -> Self {
        Self {
            addr: addr.into(),
            client_id: "fluvio".to_owned(),
            connector,
            use_spu_local_address,
        }
    }

    #[allow(clippy::box_default)]
    pub fn with_addr(addr: String) -> Self {
        Self::new(addr, Box::new(DefaultDomainConnector::default()), false)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn use_spu_local_address(&self) -> bool {
        self.use_spu_local_address
    }

    pub fn connector(&self) -> &DomainConnector {
        &self.connector
    }

    pub fn set_client_id(&mut self, id: impl Into<String>) {
        self.client_id = id.into();
    }

    pub fn set_addr(&mut self, domain: String) {
        self.addr = domain
    }

    #[instrument(skip(self))]
    pub async fn connect(self) -> Result<VersionedSocket, SocketError> {
        debug!(add = %self.addr, "try connection to");
        let socket =
            FluvioSocket::connect_with_connector(&self.addr, self.connector.as_ref()).await?;
        info!(add = %self.addr, "connect to socket");
        VersionedSocket::connect(socket, Arc::new(self)).await
    }

    /// create new config with prefix add to domain, this is useful for SNI
    #[instrument(skip(self))]
    pub fn with_prefix_sni_domain(&self, prefix: &str) -> Self {
        let new_domain = format!("{}.{}", prefix, self.connector.domain());
        debug!(sni_domain = %new_domain);
        let connector = self.connector.new_domain(new_domain);

        Self {
            addr: self.addr.clone(),
            client_id: self.client_id.clone(),
            connector,
            use_spu_local_address: self.use_spu_local_address,
        }
    }

    pub fn recreate(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            client_id: self.client_id.clone(),
            connector: self
                .connector
                .new_domain(self.connector.domain().to_owned()),
            use_spu_local_address: self.use_spu_local_address,
        }
    }
}

/// wrap around versions
#[derive(Clone, Debug)]
pub struct Versions {
    api_versions: ApiVersions,
    platform_version: semver::Version,
}

impl Versions {
    pub fn new(version_response: ApiVersionsResponse) -> Self {
        Self {
            api_versions: version_response.api_keys,
            platform_version: version_response.platform_version.to_semver(),
        }
    }

    /// Tells the platform version reported by the SC
    ///
    /// The platform version refers to the value in the VERSION
    /// file at the time the SC was compiled.
    pub fn platform_version(&self) -> &semver::Version {
        &self.platform_version
    }

    /// Given an API key, it returns maximum compatible version. None if not found
    pub fn lookup_version<R: Request>(&self) -> Option<i16> {
        for version in &self.api_versions {
            if version.api_key == R::API_KEY as i16 {
                // try to find most latest maximum version
                if version.max_version >= R::MIN_API_VERSION
                    && version.min_version <= R::MAX_API_VERSION
                {
                    return Some(R::MAX_API_VERSION.min(version.max_version));
                }
            }
        }

        None
    }
}

/// Connection that perform request/response
pub struct VersionedSerialSocket {
    socket: SharedMultiplexerSocket,
    config: Arc<ClientConfig>,
    versions: Versions,
}

impl Deref for VersionedSerialSocket {
    type Target = SharedMultiplexerSocket;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}

impl fmt::Display for VersionedSerialSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "config: {}, {:?}", self.config, self.socket)
    }
}
unsafe impl Send for VersionedSerialSocket {}

impl VersionedSerialSocket {
    pub fn new(
        socket: SharedMultiplexerSocket,
        config: Arc<ClientConfig>,
        versions: Versions,
    ) -> Self {
        Self {
            socket,
            config,
            versions,
        }
    }

    pub fn versions(&self) -> &Versions {
        &self.versions
    }

    /// get new socket
    pub fn new_socket(&self) -> SharedMultiplexerSocket {
        self.socket.clone()
    }

    /// Check if inner socket is stale
    pub fn is_stale(&self) -> bool {
        self.socket.is_stale()
    }

    fn check_liveness(&self) -> Result<(), SocketError> {
        if self.is_stale() {
            Err(SocketError::SocketStale)
        } else {
            Ok(())
        }
    }

    /// send and wait for reply serially
    #[instrument(level = "trace", skip(self, request))]
    pub async fn send_receive<R>(&self, request: R) -> Result<R::Response, SocketError>
    where
        R: Request + Send + Sync,
    {
        self.check_liveness()?;

        let req_msg = self.new_request(request, self.versions.lookup_version::<R>());

        // send request & save response
        self.socket.send_and_receive(req_msg).await
    }

    /// send and do not wait for reply
    #[instrument(level = "trace", skip(self, request))]
    pub async fn send_async<R>(&self, request: R) -> Result<AsyncResponse<R>, SocketError>
    where
        R: Request + Send + Sync,
    {
        self.check_liveness()?;

        let req_msg = self.new_request(request, self.lookup_version::<R>());

        // send request & get a Future that resolves to response
        self.socket.send_async(req_msg).await
    }

    /// look up version for the request
    pub fn lookup_version<R>(&self) -> Option<Version>
    where
        R: Request,
    {
        self.versions.lookup_version::<R>()
    }

    /// send, wait for reply and retry if failed
    #[instrument(level = "trace", skip(self, request))]
    pub async fn send_receive_with_retry<R, I>(
        &self,
        request: R,
        retries: I,
    ) -> Result<R::Response, SocketError>
    where
        R: Request + Send + Sync + Clone,
        I: IntoIterator<Item = Duration> + Debug + Send,
    {
        self.check_liveness()?;

        let req_msg = self.new_request(request, self.versions.lookup_version::<R>());

        // send request & retry it if result is Err
        retry_if(
            retries,
            || self.socket.send_and_receive(req_msg.clone()),
            is_retryable,
        )
        .await
    }

    /// create new request based on version
    #[instrument(level = "trace", skip(self, request, version))]
    pub fn new_request<R>(&self, request: R, version: Option<i16>) -> RequestMessage<R>
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
}

impl SerialFrame for VersionedSerialSocket {
    fn config(&self) -> &ClientConfig {
        &self.config
    }
}

fn is_retryable(err: &SocketError) -> bool {
    use std::io::ErrorKind;
    match err {
        SocketError::Io { source, .. } => matches!(
            source.kind(),
            ErrorKind::AddrNotAvailable
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::NotConnected
                | ErrorKind::Other
                | ErrorKind::TimedOut
                | ErrorKind::UnexpectedEof
                | ErrorKind::Interrupted
        ),

        SocketError::SocketClosed | SocketError::SocketStale => false,
    }
}

#[cfg(test)]
mod test {
    use fluvio_protocol::Decoder;
    use fluvio_protocol::Encoder;
    use fluvio_protocol::api::Request;
    use fluvio_protocol::link::versions::ApiVersionKey;

    use super::ApiVersionsResponse;
    use super::Versions;

    #[derive(Encoder, Decoder, Default, Debug)]
    struct T1;

    impl Request for T1 {
        const API_KEY: u16 = 1000;
        const MIN_API_VERSION: i16 = 6;
        const MAX_API_VERSION: i16 = 9;

        type Response = u8;
    }

    #[derive(Encoder, Decoder, Default, Debug)]
    struct T2;

    impl Request for T2 {
        const API_KEY: u16 = 1000;
        const MIN_API_VERSION: i16 = 2;
        const MAX_API_VERSION: i16 = 3;

        type Response = u8;
    }

    #[test]
    fn test_version_lookup() {
        let mut response = ApiVersionsResponse::default();

        response.api_keys.push(ApiVersionKey {
            api_key: 1000,
            min_version: 5,
            max_version: 10,
        });

        let versions = Versions::new(response);

        // None if api_key not found
        assert_eq!(versions.lookup_version::<T1>(), Some(9));
        assert_eq!(versions.lookup_version::<T2>(), None);
    }
}
