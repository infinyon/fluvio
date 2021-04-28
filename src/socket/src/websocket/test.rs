use wasm_bindgen_test::*;
use crate::{
    AllFlvSocket,
    AsyncResponse,
    WebSocketConnector,
    AllMultiplexerSocket,
};
use std::io::Cursor;
use bytes::BufMut;
use bytes::Bytes;
use futures_util::future::join;
use futures_util::{SinkExt, StreamExt};
use tracing::debug;
use tracing::info;
use std::time::Duration;

use crate::FlvSocket;
use crate::FlvSocketError;
use fluvio_future::test_async;
use fluvio_future::timer::sleep;
use fluvio_protocol::{Decoder, Encoder};

#[wasm_bindgen_test]
async fn setup_client() {
    let addr = "localhost:3030";
    sleep(Duration::from_millis(50)).await;
    debug!("client: trying to connect");
    let mut socket = FlvSocket::connect(&addr).await.expect("connect");
    info!("client: connect to test server and waiting...");
    let stream = socket.get_mut_stream();
    let next_value = stream.get_mut_tcp_stream().next().await;
    debug!("client: got bytes");
    let bytes = next_value.expect("next").expect("bytes");
    assert_eq!(bytes.len(), 7);
    debug!("decoding values");
    let mut src = Cursor::new(&bytes);
    let mut msg1 = String::new();
    msg1.decode(&mut src, 0).expect("decode should work");
    assert_eq!(msg1, "hello");

}
