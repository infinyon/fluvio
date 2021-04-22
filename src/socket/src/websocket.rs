use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::sync::Arc;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use web_sys::WebSocket;

use crate::error::FlvSocketError;
use log::*;

#[derive(Clone)]
pub enum WebSocketConnector {
    Simple
}

impl Default for WebSocketConnector {
    fn default() -> Self {
        Self::Simple
    }
}


pub struct FluvioWebSocket {
    inner: WebSocket,
    sink: InnerWebsocketSink,
    stream: InnerWebsocketStream,
}

unsafe impl Send for FluvioWebSocket {}
unsafe impl Sync for FluvioWebSocket {}

impl FluvioWebSocket {
    pub async fn connect_with_connector(
        addr: &str,
        connector: &WebSocketConnector,
    ) -> Result<Self, FlvSocketError> {
        unimplemented!();
    }
    pub async fn send_and_receive<R>(
        &self,
        mut req_msg: RequestMessage<R>,
    ) -> Result<R::Response, FlvSocketError>
    where
        R: Request,
    {
        unimplemented!();
    }

    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request,
    {
        unimplemented!();
    }

    pub fn get_mut_sink(&mut self) -> &mut InnerWebsocketSink {
        unimplemented!();
        //&mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut InnerWebsocketStream {
        unimplemented!();
        //&mut self.stream
    }
}

pub struct InnerWebsocketSink;
impl InnerWebsocketSink {
    pub async fn send_request<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<(), FlvSocketError>
    where
        RequestMessage<R>: Encoder + Default + Debug,
    {
        unimplemented!();
    }
}
pub struct InnerWebsocketStream;
impl InnerWebsocketStream {
    pub async fn next_response<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request,
    {
        unimplemented!();
    }
}

pub struct MultiplexerWebsocket {
    socket: FluvioWebSocket,
}

impl MultiplexerWebsocket {
    pub fn shared(socket: FluvioWebSocket) -> Arc<Self> {
        Arc::new(Self::new(socket))
    }
    pub fn new(socket: FluvioWebSocket) -> Self {
        Self { socket }
    }
    pub async fn send_and_receive<R>(
        &self,
        mut req_msg: RequestMessage<R>,
    ) -> Result<R::Response, FlvSocketError>
    where
        R: Request,
    {
        unimplemented!();
    }
    pub async fn create_stream<R>(
        &self,
        mut req_msg: RequestMessage<R>,
        queue_len: usize,
    ) -> Result<AsyncResponse<R>, FlvSocketError>
    where
        R: Request,
    {
        unimplemented!();
    }
}
use core::task::{Context, Poll};
use async_channel::bounded;
use async_channel::Receiver;
use pin_project::{pin_project, pinned_drop};
use std::io::Cursor;
use std::pin::Pin;
use futures_util::stream::{Stream, StreamExt};
use std::marker::PhantomData;
use bytes::BytesMut;
/// Implement async socket where response are send back async manner
/// they are queued using channel
#[pin_project(PinnedDrop)]
pub struct AsyncResponse<R> {
    #[pin]
    receiver: Receiver<Option<BytesMut>>,
    header: RequestHeader,
    correlation_id: i32,
    data: PhantomData<R>,
}

#[pinned_drop]
impl<R> PinnedDrop for AsyncResponse<R> {
    fn drop(self: Pin<&mut Self>) {
        self.receiver.close();
        debug!("multiplexer stream: {} closed", self.correlation_id);
    }
}

impl<R: Request> Stream for AsyncResponse<R> {
    type Item = Result<R::Response, FlvSocketError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next: Option<Option<_>> = match this.receiver.poll_next(cx) {
            Poll::Pending => {
                trace!("Waiting for async response");
                return Poll::Pending;
            }
            Poll::Ready(next) => next,
        };

        let bytes = if let Some(bytes) = next {
            bytes
        } else {
            return Poll::Ready(None);
        };

        let bytes = if let Some(bytes) = bytes {
            bytes
        } else {
            return Poll::Ready(Some(Err(FlvSocketError::SocketClosed)));
        };

        let mut cursor = Cursor::new(&bytes);
        let response = R::Response::decode_from(&mut cursor, this.header.api_version());

        let value = match response {
            Ok(value) => {
                trace!("Received response bytes: {},  {:#?}", bytes.len(), &value,);
                Some(Ok(value))
            }
            Err(e) => Some(Err(e.into())),
        };
        Poll::Ready(value)
    }
}
