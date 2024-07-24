use bytes::BufMut;
use http::Request;
use tracing::instrument;
use anyhow::Result;
use ureq::OrAnyStatus;

#[instrument]
pub async fn get_bytes_req<T: std::fmt::Debug>(req: &Request<T>) -> Result<bytes::Bytes> {
    let uri = req.uri().to_string();
    get_bytes(&uri).await
}

#[instrument]
pub async fn get_bytes(uri: &str) -> Result<bytes::Bytes> {
    let req = ureq::get(uri);
    let resp = req
        .call()
        .or_any_status()
        .map_err(|e| anyhow::anyhow!("get transport error : {e}"))?;

    let len: usize = match resp.header("Content-Length") {
        Some(hdr) => hdr.parse()?,
        None => 0usize,
    };

    let mut bytes_writer = bytes::BytesMut::with_capacity(len).writer();

    std::io::copy(&mut resp.into_reader(), &mut bytes_writer)?;

    Ok(bytes_writer.into_inner().freeze())
}

#[instrument]
pub async fn get_simple(uri: &str) -> Result<String> {
    let body_bytes = get_bytes(uri).await?;
    let body = std::str::from_utf8(&body_bytes)?;
    Ok(body.to_string())
}
