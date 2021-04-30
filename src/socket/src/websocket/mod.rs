use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::sync::Arc;

#[cfg(test)]
mod test;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use web_sys::{ErrorEvent, MessageEvent};
use wasm_bindgen::{
    JsCast,
    JsValue,
    closure::Closure,
};

use crate::error::FlvSocketError;
use crate::multiplexing::AsyncResponse;
use log::*;
use fluvio_future::timer::sleep;
use futures_util::StreamExt;
use std::time::Duration;

use std::io::Cursor;

#[derive(Clone)]
pub enum WebSocketConnector {
    Simple
}

impl Default for WebSocketConnector {
    fn default() -> Self {
        Self::Simple
    }
}

use ws_stream_wasm::{
    WsMeta,
    WsStream,
    WsMessage,
};
use wasm_bindgen::UnwrapThrowExt;

pub struct FluvioWebSocket {
    inner: WsMeta,
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
        let (mut ws, wsio) = WsMeta::connect(addr, None).await.expect_throw("Could not create websocket");

        Ok(Self {
            inner: ws,
            sink: InnerWebsocketSink,
            stream: InnerWebsocketStream::new(wsio),
        })
    }
    pub async fn connect(addr: &str) -> Result<Self, FlvSocketError> {
        Self::connect_with_connector(addr, &WebSocketConnector::default()).await
    }
    pub async fn send_and_receive<R>(
        &self,
        mut req_msg: RequestMessage<R>,
    ) -> Result<R::Response, FlvSocketError>
    where
        R: Request + Send + Sync,
    {
        unimplemented!();
    }

    pub async fn send<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request + Send + Sync,
    {
        let bytes = req_msg.as_bytes(0)?;
        self.inner.wrapped().send_with_u8_array(&bytes)?;
        self.stream.next_response(req_msg).await
    }

    pub fn get_mut_sink(&mut self) -> &mut InnerWebsocketSink {
        unimplemented!();
        //&mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut InnerWebsocketStream {
        &mut self.stream
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
use tokio_util::codec::Framed;
use tokio_util::compat::Compat;
use futures_util::stream::{
    SplitStream,
    Stream,
};
use fluvio_protocol::codec::FluvioCodec;
use std::io::ErrorKind;

use futures_util::io::{AsyncRead, AsyncWrite};

pub struct InnerWebsocketStream {
    ws: WsStream,
}
unsafe impl Send for InnerWebsocketStream {}
unsafe impl Sync for InnerWebsocketStream {}

impl InnerWebsocketStream {
    pub fn new(ws: WsStream) -> Self {
        Self {
            ws,
        }
    }
    pub async fn next_response<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request + Send + Sync,
    {
        /*
        */
        match self.ws.next().await {
            Some(WsMessage::Binary(data)) => {
                let response = req_msg.decode_response(
                    &mut Cursor::new(&data),
                    req_msg.header.api_version(),
                )?;
                trace!("received {} bytes: {:#?}", data.len(), &response);
                Ok(response)
            }
            None => Err(
                IoError::new(
                    ErrorKind::UnexpectedEof, "server has terminated connection"
                ).into()),
            _ => unreachable!()
        }
        //unimplemented!();
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
