#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::io::RawFd;

use tracing::debug;
use futures_util::StreamExt;
use tokio_util::codec::Framed;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures_util::io::{AsyncRead, AsyncWrite};

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
use fluvio_protocol::codec::FluvioCodec;

use fluvio_future::net::TcpStream;
use fluvio_future::net::TcpDomainConnector;
use fluvio_future::net::DefaultTcpDomainConnector;
use fluvio_future::tls::AllTcpStream;

use crate::InnerKfSink;
use crate::InnerKfStream;
use super::KfSocketError;

pub type KfSocket = InnerKfSocket<TcpStream>;
pub type AllKfSocket = InnerKfSocket<AllTcpStream>;

/// KfSocket is high level socket that can send and receive kf-protocol
#[derive(Debug)]
pub struct InnerKfSocket<S> {
    sink: InnerKfSink<S>,
    stream: InnerKfStream<S>,
    stale: bool,
}

unsafe impl<S> Sync for InnerKfSocket<S> {}

impl<S> InnerKfSocket<S> {
    pub fn new(sink: InnerKfSink<S>, stream: InnerKfStream<S>) -> Self {
        InnerKfSocket {
            sink,
            stream,
            stale: false,
        }
    }

    pub fn split(self) -> (InnerKfSink<S>, InnerKfStream<S>) {
        (self.sink, self.stream)
    }

    /// mark as stale
    pub fn set_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn get_mut_sink(&mut self) -> &mut InnerKfSink<S> {
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut InnerKfStream<S> {
        &mut self.stream
    }
}

impl<S> InnerKfSocket<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// connect to target address with connector
    pub async fn connect_with_connector<C>(addr: &str, connector: &C) -> Result<Self, KfSocketError>
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
        Self::new(InnerKfSink::new(sink, raw_fd), stream.into())
    }

    /// as client, send request and wait for reply from server
    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, KfSocketError>
    where
        R: Request,
    {
        self.sink.send_request(&req_msg).await?;

        self.stream.next_response(&req_msg).await
    }
}

impl<S> From<(InnerKfSink<S>, InnerKfStream<S>)> for InnerKfSocket<S> {
    fn from(pair: (InnerKfSink<S>, InnerKfStream<S>)) -> Self {
        let (sink, stream) = pair;
        InnerKfSocket::new(sink, stream)
    }
}

impl KfSocket {
    pub async fn connect(addr: &str) -> Result<Self, KfSocketError> {
        Self::connect_with_connector(addr, &DefaultTcpDomainConnector::new()).await
    }
}

impl From<TcpStream> for KfSocket {
    fn from(tcp_stream: TcpStream) -> Self {
        let fd = tcp_stream.as_raw_fd();
        Self::from_stream(tcp_stream, fd)
    }
}
