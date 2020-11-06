use std::io::{ErrorKind, Error as IoError};
use async_h1::client;
use http_types::{Error, Request, Response, StatusCode};
use tracing::debug;

pub async fn execute(req: Request) -> Result<Response, Error> {
    debug!("executing request: {:#?}", req);

    if req.url().scheme() != "https" {
        return Err(IoError::new(ErrorKind::InvalidInput, "Must use https").into());
    }

    let host = req
        .url()
        .host_str()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing hostname"))?
        .to_string();

    let addr = req
        .url()
        .socket_addrs(|| Some(443))?
        .into_iter()
        .next()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing valid address"))?;

    let tcp_stream = fluvio_future::net::TcpStream::connect(addr).await?;
    let tls_connector = fluvio_future::tls::TlsConnector::default();
    let tls_stream = tls_connector.connect(host, tcp_stream).await?;
    let result = client::connect(tls_stream, req).await?;

    debug!("http result: {:#?}", result);
    Ok(result)
}
