use std::{
    io::Cursor,
    io::Result,
    net::{SocketAddr, TcpStream},
};
use std::io::prelude::*;

use tracing::debug;

use fluvio_protocol::{Encoder, api::RequestMessage};
use dataplane::versions::ApiVersionsRequest;

pub fn probe(endpoint: SocketAddr) -> Result<()> {
    let mut stream = TcpStream::connect(endpoint)?;
    let req_msg = RequestMessage::new_request(ApiVersionsRequest::default());
    let mut response_bytes = [0u8; 512];
    stream.write(&req_msg.as_bytes(0)?)?;
    let _len = stream.read(&mut response_bytes)?;
    let mut cursor = Cursor::new(response_bytes);
    cursor.set_position(4);
    let response = req_msg.decode_response(&mut cursor, req_msg.header.api_version())?;
    debug!("Probe Response {:?}", response);
    Ok(())
}
