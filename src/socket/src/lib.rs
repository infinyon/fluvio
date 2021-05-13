mod error;
pub use self::error::FlvSocketError;
mod response;
pub use response::AsyncResponse;
use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
mod multiplexing;
pub use fluvio_future::net::{BoxConnection, Connection};
pub use multiplexing::*;
mod socket;
pub use socket::*;
mod sink;
pub use sink::ExclusiveFlvSink;



cfg_if::cfg_if! {
    if #[cfg(unix)] {

        #[cfg(test)]
        pub mod test_request;
        pub use sink::*;
        mod stream;
        pub use stream::FluvioStream;
        pub use stream::*;

        pub use self::socket::FluvioSocket;

        /// send request and return response from calling server at socket addr
        pub async fn send_and_receive<R>(
            addr: &str,
            request: &RequestMessage<R>,
        ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
        where
            R: Request,
        {
            let mut client = FluvioSocket::connect(addr).await?;

            let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

            Ok(msgs)
        }

    } else if #[cfg(target_arch = "wasm32")] {
        mod websocket;
        pub use websocket::{
            //MultiplexerWebsocket as MultiplexerSocket,
            //FluvioWebSocket as FluvioSocket,
            WebSocketConnector as DomainConnector,
            WebsocketSink as FluvioSink,
            WebsocketStream as FluvioStream,
        };
    }

}

use std::sync::Arc;
pub type SharedMultiplexerSocket = Arc<MultiplexerSocket>;
