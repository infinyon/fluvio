use std::fmt::Display;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use log::trace;
use futures::stream::StreamExt;
use futures_codec::Framed;

use flv_future_aio::net::ToSocketAddrs;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::transport::KfCodec;

use flv_future_aio::net::AsyncTcpStream;

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
    pub async fn connect<A>(addr: A) -> Result<KfSocket, KfSocketError>
    where
        A: ToSocketAddrs + Display,
    {
        trace!("trying to connect to server at: {}", addr);
        let tcp_stream = AsyncTcpStream::connect(addr).await?;
        Ok(tcp_stream.into())
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

impl From<AsyncTcpStream> for KfSocket {
    fn from(tcp_stream: AsyncTcpStream) -> Self {
        let fd = tcp_stream.as_raw_fd();
        let framed = Framed::new(tcp_stream, KfCodec {});
        let (sink, stream) = framed.split();
        KfSocket {
            sink: KfSink::new(sink, fd),
            stream: stream.into(),
            stale: false,
        }
    }
}

impl From<(KfSink, KfStream)> for KfSocket {
    fn from(pair: (KfSink, KfStream)) -> Self {
        let (sink, stream) = pair;
        KfSocket::new(sink, stream)
    }
}
