use std::{
    fmt::Debug,
    time::{Duration, Instant},
    path::Path,
};

use fluvio_future::timer::sleep;
use http::uri::Scheme;
use isahc::{
    AsyncBody, Request, Response,
    config::{ClientCertificate, CaCertificate, PrivateKey},
    prelude::Configurable,
};
use tracing::{debug, error, instrument};
use anyhow::Result;

use crate::error::HttpError;

#[instrument(
    skip(request),
    fields(uri = %request.uri())
)]
pub async fn execute<B: Into<AsyncBody> + Debug>(
    request: Request<B>,
) -> Result<Response<AsyncBody>> {
    debug!(?request, "Executing http request:");

    if request.uri().scheme() != Some(&Scheme::HTTPS) {
        error!("CLI http executor only accepts https!");
        return Err(HttpError::InvalidInput("Must use https".to_string()).into());
    }

    let host = request
        .uri()
        .host()
        .ok_or_else(|| HttpError::InvalidInput("missing hostname".to_string()))?
        .to_string();
    debug!(%host, "Valid hostname:");

    let response = isahc::send_async(request).await?;

    debug!(?response, "Http response:");
    Ok(response)
}

pub async fn read_to_end(response: Response<AsyncBody>) -> std::io::Result<Vec<u8>> {
    use futures::io::AsyncReadExt;
    let mut async_body = response.into_body();
    let mut body = Vec::with_capacity(async_body.len().unwrap_or_default() as usize);
    async_body.read_to_end(&mut body).await?;
    Ok(body)
}

pub async fn wait_http_ready(url: &str, timeout: Duration) -> Result<bool> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let request = http::Request::get(url).body(())?;
        if let Ok(response) = isahc::send_async(request).await {
            let status = response.status();
            if !status.is_client_error() && !status.is_server_error() {
                return Ok(true);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(false)
}

pub async fn wait_https_ready(
    url: &str,
    timeout: Duration,
    ca_cert: Option<&Path>,
    client_cert: Option<&Path>,
    client_private_key: Option<&Path>,
) -> anyhow::Result<bool> {
    let started = Instant::now();
    let ca_cert = ca_cert.map(CaCertificate::file);
    let client_cert = client_cert.map(|p| {
        ClientCertificate::pem_file(
            p,
            client_private_key.map(|p_path| PrivateKey::pem_file(p_path, None)),
        )
    });
    while started.elapsed() < timeout {
        let builder = http::Request::get(url);
        let builder = match ca_cert.clone() {
            Some(ca_cert) => builder.ssl_ca_certificate(ca_cert),
            None => builder,
        };
        let builder = match client_cert.clone() {
            Some(client_cert) => builder.ssl_client_certificate(client_cert),
            None => builder,
        };
        let request = builder.body(())?;
        if let Ok(response) = isahc::send_async(request).await {
            let status = response.status();
            if !status.is_client_error() && !status.is_server_error() {
                return Ok(true);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[fluvio_future::test]
    async fn test_web_request() {
        use fluvio_index::HttpAgent;
        use http::StatusCode;
        let agent = HttpAgent::default();
        let index = agent.request_index().expect("Failed to get request index");
        let response = execute(index).await.expect("Failed to execute request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[fluvio_future::test]
    async fn test_https_required() {
        //given
        let request = Request::get("http://fluvio.io")
            .body(())
            .expect("valid url");

        //when
        let result = execute(request).await;

        //then
        match result {
            Err(err) => match err.downcast_ref::<HttpError>() {
                Some(HttpError::InvalidInput(_)) => {}
                None => panic!("Expected invalid input error"),
            },
            Ok(_) => panic!("Expected error"),
        }
    }

    #[fluvio_future::test]
    async fn test_read_empty_body() {
        //given
        let body = AsyncBody::empty();
        let response = Response::new(body);

        //when
        let result = read_to_end(response).await.expect("read body");

        //then
        assert!(result.is_empty());
    }

    #[fluvio_future::test]
    async fn test_read_body() {
        //given
        let body = AsyncBody::from("content");
        let response = Response::new(body);

        //when
        let result = read_to_end(response).await.expect("read body");

        //then
        assert_eq!(&result, b"content");
    }
}
