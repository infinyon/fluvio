
use std::fmt;

use tracing::debug;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;

use fluvio_future::net::{
    BoxWriteConnection, BoxReadConnection
};

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        use std::os::unix::io::AsRawFd;
        use std::os::unix::io::RawFd;
        use fluvio_future::net::{
            DefaultTcpDomainConnector, TcpDomainConnector, TcpStream,
        };
    }
}

use super::FlvSocketError;
use crate::FluvioSink;
use crate::FluvioStream;

/// Socket abstract that can send and receive fluvio objects
pub struct FluvioSocket {
    sink: FluvioSink,
    stream: FluvioStream,
    stale: bool,
}

impl fmt::Debug for FluvioSocket {
    #[cfg(unix)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fd({})", self.id())
    }
    #[cfg(target_arch = "wasm32")]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "websocket")
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

    /// as client, send request and wait for reply from server
    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request,
    {
        debug!("Sending request : {:#?}", req_msg);
        self.sink.send_request(&req_msg).await?;
        debug!("Successfully sent : {:#?}", req_msg);

        self.stream.next_response(&req_msg).await
    }
}

#[cfg(target_arch = "wasm32")]
impl FluvioSocket {
    pub async fn connect_with_connector(
        addr: &str,
        _connector: &crate::websocket::WebSocketConnector,
    ) -> Result<Self, FlvSocketError> {
        let addr = if addr == "localhost:9010" {
            "ws://localhost:3001"
        } else {
            addr
        };
        debug!("CONNECTING TO {:?}", addr);
        use ws_stream_wasm::WsMeta;
        use wasm_bindgen::UnwrapThrowExt;
        let (ws, wsio) = WsMeta::connect(addr, None).await.expect_throw("Could not create websocket");
        debug!("CONNECTED TO {:?}", addr);

        Ok(Self::new(FluvioSink::new(ws), FluvioStream::new(wsio)))
    }
}

#[cfg(unix)]
impl FluvioSocket {
    pub fn from_stream(write: BoxWriteConnection, read: BoxReadConnection, raw_fd: RawFd) -> Self {
        Self::new(FluvioSink::new(write, raw_fd), FluvioStream::new(read))
    }

    pub fn id(&self) -> RawFd {
        self.sink.id()
    }

    /// connect to target address with connector
    pub async fn connect_with_connector(
        addr: &str,
        connector: &dyn TcpDomainConnector,
    ) -> Result<Self, FlvSocketError> {
        debug!("trying to connect to addr at: {}", addr);
        let (write, read, fd) = connector.connect(addr).await?;
        Ok(Self::from_stream(write, read, fd))
    }

    pub async fn connect(addr: &str) -> Result<Self, FlvSocketError> {
        let connector = DefaultTcpDomainConnector::new();
        Self::connect_with_connector(addr, &connector).await
    }
}


impl From<(FluvioSink, FluvioStream)> for FluvioSocket {
    fn from(pair: (FluvioSink, FluvioStream)) -> Self {
        let (sink, stream) = pair;
        Self::new(sink, stream)
    }
}

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        impl From<TcpStream> for FluvioSocket {
            fn from(tcp_stream: TcpStream) -> Self {
                let fd = tcp_stream.as_raw_fd();
                Self::from_stream(Box::new(tcp_stream.clone()),Box::new(tcp_stream), fd)
            }
        }
    }
}
