use async_h1::client;
use http_types::{Request, Response, Error, StatusCode};
use tracing::debug;

pub async fn execute(req: Request) -> Result<Response, Error> {
    debug!("executing request: {:#?}", req);

    let host = req
        .url()
        .host_str()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing hostname"))?
        .to_string();

    let is_https = req.url().scheme() == "https";

    let addr = req
        .url()
        .socket_addrs(|| if is_https { Some(443) } else { Some(80) })?
        .into_iter()
        .next()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing valid address"))?;

    let tcp_stream = fluvio_future::net::TcpStream::connect(addr).await?;

    let result = if is_https {
        let tls_connector = fluvio_future::tls::TlsConnector::default();
        let tls_stream = tls_connector.connect(host, tcp_stream).await?;
        client::connect(tls_stream, req).await
    } else {
        client::connect(tcp_stream, req).await
    };

    debug!("http result: {:#?}", result);
    result
}
