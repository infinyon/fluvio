pub use http;
pub use http::StatusCode;

pub use fluvio_future::http_client::Client;
pub use fluvio_future::http_client::ResponseExt;
pub use fluvio_future::http_client::{get, send};
pub use mime;

use anyhow::{anyhow, Result};

pub async fn get_auth_json<J: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> Result<J> {
    let req = http::Request::get(url)
        .header("Authorization", auth_token)
        .body("")
        .map_err(|e| anyhow!("request format error {e}"))?;

    let resp = send(req)
        .await
        .map_err(|e| anyhow!("http access error {e}"))?;

    let data = resp
        .json::<J>()
        .await
        .map_err(|e| anyhow!("json parse error {e}"))?;

    Ok(data)
}
