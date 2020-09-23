use async_h1::client;
use http_types::{Request, Response, Error, StatusCode};
use tracing::debug;

pub async fn execute(req: Request) -> Result<Response, Error> {
    debug!("executing request: {:#?}", req);

    let addr = req
        .url()
        .socket_addrs(|| Some(443))?
        .into_iter()
        .next()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing valid address"))?;

    let raw_stream = fluvio_future::net::TcpStream::connect(addr).await?;
    let result = client::connect(raw_stream, req).await;

    debug!("http result: {:#?}", result);
    result
}
