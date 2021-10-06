use std::fmt;

use tracing::{debug, instrument};

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;

use fluvio_future::net::{
    BoxReadConnection, BoxWriteConnection, ConnectionFd, DefaultDomainConnector, TcpDomainConnector,
};

use super::SocketError;
use crate::FluvioSink;
use crate::FluvioStream;

/// Socket abstract that can send and receive fluvio objects
pub struct FluvioSocket {
    sink: FluvioSink,
    stream: FluvioStream,
    stale: bool,
}

impl fmt::Debug for FluvioSocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Socket({})", self.id())
    }
}

impl FluvioSocket {
    pub fn new(sink: FluvioSink, stream: FluvioStream) -> Self {
        Self {
            sink,
            stream,
            stale: false,
        }
    }

    pub fn split(self) -> (FluvioSink, FluvioStream) {
        (self.sink, self.stream)
    }

    /// mark as stale
    pub fn set_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn get_mut_sink(&mut self) -> &mut FluvioSink {
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut FluvioStream {
        &mut self.stream
    }

    pub fn id(&self) -> ConnectionFd {
        self.sink.id()
    }

    /// as client, send request and wait for reply from server
    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, SocketError>
    where
        R: Request,
    {
        self.sink.send_request(req_msg).await?;
        self.stream.next_response(req_msg).await
    }
}

impl FluvioSocket {
    #[allow(clippy::clone_on_copy)]
    pub fn from_stream(
        write: BoxWriteConnection,
        read: BoxReadConnection,
        fd: ConnectionFd,
    ) -> Self {
        Self::new(
            FluvioSink::new(write, fd.clone()),
            FluvioStream::new(fd, read),
        )
    }

    /// connect to target address with connector
    #[instrument(skip(connector))]
    pub async fn connect_with_connector(
        addr: &str,
        connector: &dyn TcpDomainConnector,
    ) -> Result<Self, SocketError> {
        use once_cell::sync::Lazy;

        static CONNECTION_RETRIES: Lazy<u64> = Lazy::new(|| {
            use std::env;

            let var_value = env::var("FLV_CONNECTION_RETRIES").unwrap_or_default();
            let retries: u64 = var_value.parse().unwrap_or(5);
            retries
        });

        let mut attempt = 0;

        let result = loop {
            debug!("trying to connect to addr at: {}", addr);

            match connector.connect(addr).await {
                Err(_) if attempt < *CONNECTION_RETRIES => {
                    debug!(
                        "failed attempt {} to connect to {}. Sleeping 5 seconds",
                        attempt + 1,
                        addr
                    );
                    attempt += 1;
                    fluvio_future::timer::sleep(std::time::Duration::from_secs(5)).await;
                }
                res => break res,
            }
        };
        let (write, read, fd) = result?;
        Ok(Self::from_stream(write, read, fd))
    }
}

impl From<(FluvioSink, FluvioStream)> for FluvioSocket {
    fn from(pair: (FluvioSink, FluvioStream)) -> Self {
        let (sink, stream) = pair;
        Self::new(sink, stream)
    }
}

impl FluvioSocket {
    pub async fn connect(addr: &str) -> Result<Self, SocketError> {
        let connector = DefaultDomainConnector::new();
        Self::connect_with_connector(addr, &connector).await
    }
}

cfg_if::cfg_if! {
    if #[cfg(any(unix, windows))] {
        use fluvio_future::net::{
            AsConnectionFd, TcpStream,
        };
        impl From<TcpStream> for FluvioSocket {
            fn from(tcp_stream: TcpStream) -> Self {
                let fd = tcp_stream.as_connection_fd();
                Self::from_stream(Box::new(tcp_stream.clone()),Box::new(tcp_stream), fd)
            }
        }
    }
}
