mod error;
mod multiplexing;
#[cfg(not(target_arch = "wasm32"))]
mod pooling;
//#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
mod sink_pool;

mod sink;
mod socket;
mod stream;

#[cfg(test)]
pub mod test_request;

#[cfg(not(target_arch = "wasm32"))]
mod not_wasm {
    use super::*;
    pub use self::socket::FlvSocket;
    pub use pooling::*;
    pub use sink_pool::*;
}

pub use sink::*;
pub use stream::*;
pub use socket::*;
pub use multiplexing::*;

#[cfg(not(target_arch = "wasm32"))]
pub use not_wasm::*;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;

pub use self::error::FlvSocketError;

#[cfg(target_arch = "wasm32")]
mod websocket;

#[cfg(target_arch = "wasm32")]
pub use self::websocket::{
    FluvioWebSocket as AllFlvSocket,
    FluvioWebSocket as FlvSocket,
    WebSocketConnector,
    MultiplexerWebsocket as AllMultiplexerSocket,
};

/// send request and return response from calling server at socket addr
#[cfg(not(target_arch = "wasm32"))]
pub async fn send_and_receive<R>(
    addr: &str,
    request: &RequestMessage<R>,
) -> Result<ResponseMessage<R::Response>, FlvSocketError>
where
    R: Request,
{
    let mut client = FlvSocket::connect(addr).await?;

    let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

    Ok(msgs)
}
