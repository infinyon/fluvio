use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

use std::fmt;

use tracing::debug;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;

#[cfg(not(target_arch = "wasm32"))]
use fluvio_future::net::DefaultTcpDomainConnector;

#[cfg(not(target_arch = "wasm32"))]
use fluvio_future::net::TcpDomainConnector;
#[cfg(not(target_arch = "wasm32"))]
use fluvio_future::net::TcpStream;
use fluvio_future::net::{BoxConnection, DefaultTcpDomainConnector, TcpDomainConnector, TcpStream};

use super::FlvSocketError;
use crate::FlvSink;
use crate::FlvStream;

//pub type FlvSocket = InnerFlvSocket<TcpStream>;

//#[cfg(feature = "tls")]
//pub type AllFlvSocket = InnerFlvSocket<fluvio_future::native_tls::AllTcpStream>;

/// Socket abstract that can send and receive fluvio objects
pub struct FlvSocket {
    sink: FlvSink,
    stream: FlvStream,
    stale: bool,
}

impl fmt::Debug for FlvSocket {
    #[cfg(not(target_arch = "wasm32"))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fd({})", self.id())
    }
    #[cfg(target_arch = "wasm32")]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "websocket")
    }
}

impl FlvSocket {
    pub fn new(sink: FlvSink, stream: FlvStream) -> Self {
        Self {
            sink,
            stream,
            stale: false,
        }
    }

    pub fn split(self) -> (FlvSink, FlvStream) {
        (self.sink, self.stream)
    }

    /// mark as stale
    pub fn set_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn get_mut_sink(&mut self) -> &mut FlvSink {
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut FlvStream {
        &mut self.stream
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn id(&self) -> RawFd {
        self.sink.id()
    }

    /// as client, send request and wait for reply from server
    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request,
    {
        self.sink.send_request(&req_msg).await?;

        self.stream.next_response(&req_msg).await
    }
}

impl FlvSocket {
    pub fn from_stream(write: BoxConnection, raw_fd: RawFd) -> Self {
        let read = dyn_clone::clone_box(&*write);
        Self::new(FlvSink::new(write, raw_fd), FlvStream::new(read))
    }

    /// connect to target address with connector
    pub async fn connect_with_connector(
        addr: &str,
        connector: &dyn TcpDomainConnector,
    ) -> Result<Self, FlvSocketError> {
        debug!("trying to connect to addr at: {}", addr);
        let (tcp_stream, fd) = connector.connect(addr).await?;
        Ok(Self::from_stream(tcp_stream, fd))
    }
}

impl From<(FlvSink, FlvStream)> for FlvSocket {
    fn from(pair: (FlvSink, FlvStream)) -> Self {
        let (sink, stream) = pair;
        Self::new(sink, stream)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FlvSocket {
    pub async fn connect(addr: &str) -> Result<Self, FlvSocketError> {
        let connector = DefaultTcpDomainConnector::new();
        Self::connect_with_connector(addr, &connector).await
    }
}

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        impl From<TcpStream> for FlvSocket {
            fn from(tcp_stream: TcpStream) -> Self {
                let fd = tcp_stream.as_raw_fd();
                Self::from_stream(Box::new(tcp_stream), fd)
            }
        }
    }
}
