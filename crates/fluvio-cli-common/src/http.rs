// use std::fmt::Debug;

// use http::{Request, Response};
use http::Request;
// use http::uri::Scheme;
// use isahc::{AsyncBody, Request, Response};
// use tracing::{debug, error, instrument};
use tracing::instrument;
use anyhow::Result;

// use crate::error::HttpError;
use fluvio_future::http_client;
use fluvio_future::http_client::ResponseExt;

#[instrument]
pub async fn get_bytes_req<T: std::fmt::Debug>(req: &Request<T>) -> Result<bytes::Bytes> {
    let uri = req.uri().to_string();
    get_bytes(&uri).await
}

#[instrument]
pub async fn get_bytes(uri: &str) -> Result<bytes::Bytes> {
    let resp = http_client::get(&uri).await?;
    let body_bytes = resp.bytes().await?;
    Ok(body_bytes)
}

#[instrument]
pub async fn get_simple(uri: &str) -> Result<String> {
    let body_bytes = get_bytes(uri).await?;
    let body = std::str::from_utf8(&body_bytes)?;
    Ok(body.to_string())
}
