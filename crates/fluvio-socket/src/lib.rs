mod error;
mod multiplexing;
mod sink;
mod socket;
mod stream;

#[cfg(test)]
pub mod test_request;

pub use fluvio_future::net::{BoxConnection, Connection};
pub use self::error::SocketError;
pub use self::socket::FluvioSocket;
pub use multiplexing::*;
pub use sink::*;
pub use socket::*;
pub use stream::*;

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;

/// send request and return response from calling server at socket addr
pub async fn send_and_receive<R>(
    addr: &str,
    request: &RequestMessage<R>,
) -> Result<ResponseMessage<R::Response>, SocketError>
where
    R: Request,
{
    let mut client = FluvioSocket::connect(addr).await?;
    let msgs: ResponseMessage<R::Response> = client.send(request).await?;
    Ok(msgs)
}
