#![feature(generators)]

mod error;
mod pooling;
mod socket;
mod stream;
mod sink;
mod sink_pool;
mod multiplexing;

#[cfg(test)]
pub mod test_request;

pub use self::error::KfSocketError;
pub use self::socket::KfSocket;
pub use pooling::*;
pub use sink_pool::*;
pub use stream::*;
pub use sink::*;
pub use socket::*;
pub use multiplexing::*;

use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;

/// send request and return response from calling server at socket addr
pub async fn send_and_receive<R>(
    addr: &str,
    request: &RequestMessage<R>,
) -> Result<ResponseMessage<R::Response>, KfSocketError>
where
    R: Request,
{
    let mut client = KfSocket::connect(addr).await?;

    let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

    Ok(msgs)
}
