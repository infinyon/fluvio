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

use web_sys::{ErrorEvent, MessageEvent, WebSocket};
use wasm_bindgen::{
    JsCast,
    JsValue,
    closure::Closure,
};

use crate::error::FlvSocketError;
use crate::multiplexing::AsyncResponse;
use log::*;
use fluvio_future::timer::sleep;
use std::time::Duration;


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
        let ws = WebSocket::new(addr)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
        let cloned_ws = ws.clone();

        let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
            // Handle difference Text/Binary,...
            if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                debug!("message event, received arraybuffer: {:?}", abuf);
                let array = js_sys::Uint8Array::new(&abuf);
                let len = array.byte_length() as usize;
                debug!("Arraybuffer received {}bytes: {:?}", len, array.to_vec());
                // here you can for example use Serde Deserialize decode the message
                // for demo purposes we switch back to Blob-type and send off another binary message
                /*
                cloned_ws.set_binary_type(web_sys::BinaryType::Blob);
                match cloned_ws.send_with_u8_array(&vec![5, 6, 7, 8]) {
                    Ok(_) => debug!("binary message successfully sent"),
                    Err(err) => debug!("error sending message: {:?}", err),
                }
                */
            } else if let Ok(blob) = e.data().dyn_into::<web_sys::Blob>() {
                debug!("message event, received blob: {:?}", blob);
                // better alternative to juggling with FileReader is to use https://crates.io/crates/gloo-file
                let fr = web_sys::FileReader::new().unwrap();
                let fr_c = fr.clone();
                // create onLoadEnd callback
                let onloadend_cb = Closure::wrap(Box::new(move |_e: web_sys::ProgressEvent| {
                    let array = js_sys::Uint8Array::new(&fr_c.result().unwrap());
                    let len = array.byte_length() as usize;
                    debug!("Blob received {}bytes: {:?}", len, array.to_vec());
                    // here you can for example use the received image/png data
                })as Box<dyn FnMut(web_sys::ProgressEvent)>);
                fr.set_onloadend(Some(onloadend_cb.as_ref().unchecked_ref()));
                fr.read_as_array_buffer(&blob).expect("blob not readable");
                onloadend_cb.forget();
            } else if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
                debug!("message event, received Text: {:?}", txt);
            } else {
                debug!("message event, received Unknown: {:?}", e.data());
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        // set message event handler on WebSocket
        ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));

        // forget the callback to keep it alive
        onmessage_callback.forget();

        let onerror_callback = Closure::wrap(Box::new(move |e: ErrorEvent| {
            debug!("error event: {:?}", e);
        }) as Box<dyn FnMut(ErrorEvent)>);
        ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
        onerror_callback.forget();

        let cloned_ws = ws.clone();
        let onopen_callback = Closure::wrap(Box::new(move |_| {
            debug!("socket opened");
            /*
            match cloned_ws.send_with_str("ping") {
                Ok(_) => debug!("message successfully sent"),
                Err(err) => debug!("error sending message: {:?}", err),
            }
            // send off binary message
            match cloned_ws.send_with_u8_array(&vec![0, 1, 2, 3]) {
                Ok(_) => debug!("binary message successfully sent"),
                Err(err) => debug!("error sending message: {:?}", err),
            }
            */
        }) as Box<dyn FnMut(JsValue)>);
        ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
        onopen_callback.forget();
        Ok(Self {
            inner: ws,
            sink: InnerWebsocketSink,
            stream: InnerWebsocketStream,
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
        let bytes = req_msg.as_bytes(0)?;
        self.inner.send_with_u8_array(&bytes)?;
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
use tokio_util::codec::Framed;
use tokio_util::compat::Compat;
use futures_util::stream::SplitStream;
use fluvio_protocol::codec::FluvioCodec;

use futures_util::io::{AsyncRead, AsyncWrite};
type FrameStream<S> = SplitStream<Framed<Compat<S>, FluvioCodec>>;

#[derive(Debug)]
pub struct InnerFlvStream<S>(FrameStream<S>);
impl<S> InnerFlvStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    /*
    pub fn get_mut_tcp_stream(&mut self) -> &mut FrameStream<S> {
        unimplemented!();
    }
    */
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
