
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use log::trace;
use futures::Future;
use pin_utils::unsafe_pinned;

use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;


use future_aio::net::AsyncTcpStream;

use crate::KfSink;
use crate::KfStream;

use super::KfSocketError;

/// KfSocket is high level socket that can send and receive kf-protocol
#[derive(Debug)]
pub struct KfSocket {
    sink: KfSink,
    stream: KfStream,
    stale: bool,
}

unsafe impl Sync for KfSocket {}

impl KfSocket {
    pub fn new(sink: KfSink, stream: KfStream) -> Self {
        KfSocket {
            sink,
            stream,
            stale: false,
        }
    }

    /// create socket from establishing connection to server
    pub async fn connect(addr: &SocketAddr) -> Result<KfSocket, KfSocketError> {
        trace!("trying to connect to server at: {}", addr);
        let tcp_stream = AsyncTcpStream::connect(addr).await?;
        Ok(tcp_stream.into())
    }

    
    pub fn fusable_connect(addr: &SocketAddr) -> impl Future<Output=Result<KfSocket,KfSocketError>> + '_
    {
        SocketConnectFusableFuture{ inner: Self::connect(&addr) }
    }
    

    pub fn split(self) -> (KfSink, KfStream) {
        (self.sink, self.stream)
    }

    /// mark as stale
    pub fn set_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn get_mut_sink(&mut self) -> &mut KfSink {
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut KfStream {
        &mut self.stream
    }

    /// as client, send request and wait for reply from server
    pub async fn send<'a,R>(
        &'a mut self,
        req_msg: &'a RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, KfSocketError>
    where
        R: Request
    {
        self.sink.send_request(&req_msg).await?;

        self.stream.next_response(&req_msg).await
    }
}

impl From<AsyncTcpStream> for KfSocket {
    fn from(tcp_stream: AsyncTcpStream) -> Self {
        let fd = tcp_stream.as_raw_fd();
        let (sink, stream) = tcp_stream.split().as_tuple();
        KfSocket {
            sink: KfSink::new(sink, fd),
            stream: stream.into(),
            stale: false,
        }
    }
}

impl From<(KfSink,KfStream)> for KfSocket {
    fn from(pair: (KfSink,KfStream)) -> Self {
        let (sink,stream) = pair;
        KfSocket::new(sink,stream)
    }
}





/// connect future which can be fused.  this requires it need to be unpin.
/// in the future, this could be removed
struct SocketConnectFusableFuture<F> {
    inner: F
}

impl <F>Unpin for SocketConnectFusableFuture<F>{}

impl <F>SocketConnectFusableFuture<F>  {
    unsafe_pinned!(inner: F);
}

impl <F>Future for SocketConnectFusableFuture<F> where F: Future<Output=Result<KfSocket,KfSocketError>> {

    type Output = Result<KfSocket,KfSocketError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        
        self.inner().poll(cx)
    }

}

