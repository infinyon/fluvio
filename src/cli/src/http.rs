use std::io::{ErrorKind, Error as IoError};
use async_h1::client;
use http_types::{Error, Request, Response, StatusCode};
use tracing::{debug, error, instrument};

#[instrument(
    skip(request),
    fields(url = %request.url())
)]
pub async fn execute(request: Request) -> Result<Response, Error> {
    debug!(?request, "Executing http request:");

    if request.url().scheme() != "https" {
        error!("CLI http executor only accepts https!");
        return Err(IoError::new(ErrorKind::InvalidInput, "Must use https").into());
    }

    let host = request
        .url()
        .host_str()
        .ok_or_else(|| Error::from_str(StatusCode::BadRequest, "missing hostname"))?
        .to_string();
    debug!(%host, "Valid hostname:");

    let addr: (&str, u16) = (&host, request.url().port_or_known_default().unwrap_or(443));
    let tcp_stream = fluvio_future::net::TcpStream::connect(addr).await?;
    debug!("Established TCP stream");
    let tls_connector = create_tls().await;
    debug!("Created TLS connector");
    let tls_stream = tls_connector.connect(&host, tcp_stream).await?;
    debug!("Opened TLS stream from TCP stream");
    let response = client::connect(tls_stream, request).await?;

    debug!(?response, "Http response:");
    Ok(response)
}

async fn create_tls() -> fluvio_future::openssl::TlsConnector {
    fluvio_future::openssl::TlsConnector::builder().expect("Failed to get TLS connector builder").build()
}
