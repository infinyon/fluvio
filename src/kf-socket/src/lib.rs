#![feature(generators)]

mod error;
mod pooling;
mod socket;
mod stream;
mod sink;
mod sink_pool;


#[cfg(test)]
pub mod test_request;

pub use self::error::KfSocketError;
pub use self::socket::KfSocket;
pub use pooling::SocketPool;
pub use sink_pool::SinkPool;
pub use sink_pool::SharedSinkPool;
pub use stream::KfStream;
pub use stream::InnerKfStream;
pub use sink::KfSink;
pub use sink::InnerKfSink;
pub use socket::InnerKfSocket;
pub use socket::AllKfSocket;
pub use sink::ExclusiveKfSink;


use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;


/// send request and return response from calling server at socket addr
pub async fn send_and_receive<R>(
    domain: &str,
    request: &RequestMessage<R>,
) -> Result<ResponseMessage<R::Response>, KfSocketError>
where
    R: Request,
{
    let mut client = KfSocket::connect(domain).await?;

    let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

    Ok(msgs)
}
