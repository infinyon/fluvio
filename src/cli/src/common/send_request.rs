//!
//! # Send Request to a Server
//!
use std::net::SocketAddr;

use kf_protocol::api::Request;

use crate::error::CliError;
use crate::common::Connection;

/// Create connection, send request and return response
pub async fn connect_and_send_request<R>(
    server_addr: SocketAddr,
    request: R,
    version: Option<i16>,
) -> Result<R::Response, CliError>
where
    R: Request,
{
    let mut conn = Connection::new(&server_addr).await?;
    conn.send_request(request, version).await
}
