cfg_if::cfg_if! {
    if #[cfg(unix)] {
        use std::os::unix::io::AsRawFd;
        use std::os::unix::io::RawFd;
    }
}

use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::StreamExt;
use tokio_util::codec::Framed;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::debug;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
use fluvio_protocol::codec::FluvioCodec;

use fluvio_future::net::DefaultTcpDomainConnector;
use fluvio_future::net::TcpDomainConnector;
use fluvio_future::net::TcpStream;

use super::FlvSocketError;
use crate::InnerFlvSink;
use crate::InnerFlvStream;

pub type FlvSocket = InnerFlvSocket<TcpStream>;

cfg_if::cfg_if! {
    if #[cfg(feature = "tls")] {
        pub type AllFlvSocket = InnerFlvSocket<fluvio_future::rust_tls::AllTcpStream>;
    } else if #[cfg(feature  = "native_tls")] {
        pub type AllFlvSocket = InnerFlvSocket<fluvio_future::native_tls::AllTcpStream>;
    }
}

/// FlvSocket is high level socket that can send and receive fluvio protocol
#[derive(Debug)]
pub struct InnerFlvSocket<S> {
    sink: InnerFlvSink<S>,
    stream: InnerFlvStream<S>,
    stale: bool,
}

unsafe impl<S> Sync for InnerFlvSocket<S> {}

impl<S> InnerFlvSocket<S> {
    pub fn new(sink: InnerFlvSink<S>, stream: InnerFlvStream<S>) -> Self {
        Self {
            sink,
            stream,
            stale: false,
        }
    }

    pub fn split(self) -> (InnerFlvSink<S>, InnerFlvStream<S>) {
        (self.sink, self.stream)
    }

    /// mark as stale
    pub fn set_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn get_mut_sink(&mut self) -> &mut InnerFlvSink<S> {
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut InnerFlvStream<S> {
        &mut self.stream
    }
}

impl<S> InnerFlvSocket<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// connect to target address with connector
    pub async fn connect_with_connector<C>(
        addr: &str,
        connector: &C,
    ) -> Result<Self, FlvSocketError>
    where
        C: TcpDomainConnector<WrapperStream = S>,
    {
        debug!("trying to connect to addr at: {}", addr);
        let (tcp_stream, fd) = connector.connect(addr).await?;
        Ok(Self::from_stream(tcp_stream, fd))
    }

    pub fn from_stream(tcp_stream: S, raw_fd: RawFd) -> Self {
        let framed = Framed::new(tcp_stream.compat(), FluvioCodec {});
        let (sink, stream) = framed.split();
        Self::new(InnerFlvSink::new(sink, raw_fd), stream.into())
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

impl<S> From<(InnerFlvSink<S>, InnerFlvStream<S>)> for InnerFlvSocket<S> {
    fn from(pair: (InnerFlvSink<S>, InnerFlvStream<S>)) -> Self {
        let (sink, stream) = pair;
        Self::new(sink, stream)
    }
}

impl FlvSocket {
    pub async fn connect(addr: &str) -> Result<Self, FlvSocketError> {
        Self::connect_with_connector(addr, &DefaultTcpDomainConnector::new()).await
    }
}

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        impl From<TcpStream> for FlvSocket {
            fn from(tcp_stream: TcpStream) -> Self {
                let fd = tcp_stream.as_raw_fd();
                Self::from_stream(tcp_stream, fd)
            }
        }
    }
}
