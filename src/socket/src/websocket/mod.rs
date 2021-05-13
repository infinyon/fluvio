use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};

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
use crate::AsyncResponse;
use tracing::*;
use fluvio_future::timer::sleep;
use futures_util::StreamExt;
use std::time::Duration;

use tokio_util::codec::Framed;
use tokio_util::compat::Compat;
use futures_util::stream::{
    SplitStream,
    Stream,
};
use fluvio_protocol::codec::FluvioCodec;
use std::io::ErrorKind;

use futures_util::io::{AsyncRead, AsyncWrite};
use std::io::Cursor;
use fluvio_protocol::Version;
use fluvio_protocol::Encoder as FlvEncoder;
use ws_stream_wasm::{
    WsMeta,
    WsStream,
    WsMessage,
};
use wasm_bindgen::UnwrapThrowExt;
use bytes::BytesMut;

#[derive(Clone)]
pub enum WebSocketConnector {
    Simple
}
impl WebSocketConnector {
    pub fn new_domain(&self, _domain: String) -> WebSocketConnector {
        self.clone()
	}
	pub fn domain(&self) -> &str {
		"localhost"

	}
}

impl Default for WebSocketConnector {
    fn default() -> Self {
        Self::Simple
    }
}

pub struct WebsocketSink {
    inner: WsMeta,
}
impl WebsocketSink {
    pub fn new(inner: WsMeta) -> Self {
        Self {
            inner
        }
    }
    pub fn as_shared(self) -> crate::ExclusiveFlvSink {
        crate::ExclusiveFlvSink::new(self)
    }

    pub async fn send_request<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<(), FlvSocketError>
    where
        RequestMessage<R>: Encoder + Default + Debug,
    {
        let bytes = req_msg.as_bytes(0)?;
        debug!("REQUEST: {:#?} bytes : {:?}", req_msg, bytes.to_vec());
        Ok(self.inner.wrapped().send_with_u8_array(&bytes)?)
    }

    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), FlvSocketError>
    where
        ResponseMessage<P>: FlvEncoder + Default + Debug,
    {
        let bytes = resp_msg.as_bytes(version)?;
        debug!("sending response {:#?}, bytes: {} - {:?}", &resp_msg, bytes.len(), bytes.to_vec());
        Ok(self.inner.wrapped().send_with_u8_array(&bytes)?)
    }
}

pub struct WebsocketStream {
    ws: WsStream,
}

impl WebsocketStream {
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
        R: Request,
    {
        match self.next().await {
            Some(Ok(data)) => {
                debug!("WEBSOCKET received bytes: {:?}", data);
                //let data = data.split_off(4);
                let response = req_msg.decode_response(
                    &mut Cursor::new(&data),
                    req_msg.header.api_version(),
                );
                debug!("received {} response: {:#?}", data.len(), &response);
                Ok(response?)
            },
            _ => {
                debug!("WE DIDN'T GET ANYTHING IN RESPONSE");
                Err(
                IoError::new(
                    ErrorKind::UnexpectedEof, "server has terminated connection"
                ).into())
            },
        }
    }
    // This is a wrapper around `WsStream` `StreamExt` to match the TCP socket return.a
    // The return type is `Option<Result<BytesMut,String>>` but we only actually care about the
    // `Option<Vec<u8>>`
    pub async fn next(&mut self) -> Option<Result<BytesMut, String>> {
        debug!("Waiting for next websocket message");
        match self.ws.next().await {
            Some(WsMessage::Binary(mut data)) => {
                // TODO: Fix when https://github.com/infinyon/fluvio/issues/1075 is fixed
                let data = data.split_off(4);
                debug!("Got a new message: {:?}", data);
                Some(Ok(BytesMut::from(data.as_slice())))
            }
            _ => None
        }

    }
}
