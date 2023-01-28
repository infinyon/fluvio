use std::fmt::Debug;

use http::uri::Scheme;
use isahc::{AsyncBody, Request, Response};
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
