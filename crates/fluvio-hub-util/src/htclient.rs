pub use http;
pub use http::StatusCode;
pub use http::{Request, Response};

#[cfg(not(target_arch = "wasm32"))]
use std::env;

use anyhow::{anyhow, Result};
#[cfg(not(target_arch = "wasm32"))]
use anyhow::Context;
use serde::de::DeserializeOwned;

#[cfg(not(target_arch = "wasm32"))]
use ureq::{Agent, AgentBuilder, Proxy, OrAnyStatus};

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
        .map_err(|e| anyhow!("json parse error {e}"))?;

    Ok(data)
}

/// for simple get requests
#[cfg(not(target_arch = "wasm32"))]
pub async fn get(uri: impl AsRef<str>) -> Result<Response<Vec<u8>>> {
    use std::io::Read;

    let uri = uri.as_ref();
    let agent = configure_ureq_proxy()?; // Create agent with proxy

    let req = agent.get(uri);
    let resp = req
        .call()
        .or_any_status()
        .map_err(|e| anyhow!("get transport error : {e}"))?;

    let status = resp.status();
    let len: usize = match resp.header("Content-Length") {
        Some(hdr) => hdr.parse()?,
        None => 0usize,
    };

    let mut bytes: Vec<u8> = Vec::with_capacity(len);
    resp.into_reader().read_to_end(&mut bytes)?;

    let response = Response::builder().status(status).body(bytes)?;

    Ok(response)
}

#[cfg(target_arch = "wasm32")]
pub async fn get(uri: impl AsRef<str>) -> Result<Response<Vec<u8>>> {
    let request = reqwest::Client::new().get(uri.as_ref());
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    let bytes = bytes.to_vec();
    let response = Response::builder().status(status).body(bytes)?;

    Ok(response)
}

#[cfg(target_arch = "wasm32")]
pub async fn send<T>(request: Request<T>) -> Result<Response<Vec<u8>>>
where
    T: Into<Vec<u8>> + std::fmt::Debug,
    reqwest::Request: TryFrom<http::Request<T>>,
{
    let client = reqwest::Client::new();
    let reqwest_req = reqwest::Request::try_from(request)
        .map_err(|_| anyhow::anyhow!("Failed to build Request on conversion."))?;
    let response = client
        .execute(reqwest_req)
        .await
        .map_err(|err| anyhow::anyhow!("Failed to execute request: {:?}", err))?;
    let status = response.status();
    let bytes = response.bytes().await?;
    let bytes = bytes.to_vec();
    let response = Response::builder()
        .status(status)
        .body(bytes)
        .map_err(|_| anyhow::anyhow!("Failed to build Response on conversion."))?;

    Ok(response)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn send<T>(request: Request<T>) -> Result<Response<Vec<u8>>>
where
    T: Into<Vec<u8>> + std::fmt::Debug,
{
    let (parts, body) = request.into_parts();
    let agent = configure_ureq_proxy()?; // Create agent with proxy
    let mut ureq_request = agent.request(parts.method.as_ref(), &parts.uri.to_string());
    for (name, value) in parts.headers {
        let Some(name) = name else {
            continue;
        };
        ureq_request = ureq_request.set(name.as_ref(), value.to_str().unwrap());
    }

    let body_u8: Vec<u8> = body.into();
    let response = ureq_request
        .send_bytes(&body_u8)
        .or_any_status()
        .map_err(|e| anyhow!("error: {e}"))?;
    Ok(response.into())
}

/// Configures a `ureq::Agent` with a proxy, if one is defined in the environment.
//  TODO: If `ureq` version is updated to 3.0.8, you can replace this function with `try_from_env` here, see more [PR #4438]
#[cfg(not(target_arch = "wasm32"))]
fn configure_ureq_proxy() -> Result<Agent> {
    let agent_builder = AgentBuilder::new();

    let proxy_vars = [
        ("ALL_PROXY", "all_proxy", "ALL"),
        ("HTTPS_PROXY", "https_proxy", "HTTPS"),
        ("HTTP_PROXY", "http_proxy", "HTTP"),
    ];

    let proxy_creation = |proxy_str: &str, proxy_type: &str| -> Result<Proxy> {
        Proxy::new(proxy_str).with_context(|| format!("Failed to create {proxy_type} proxy"))
    };

    for &(upper_var, lower_var, proxy_type) in &proxy_vars {
        if let Ok(proxy_str) = env::var(upper_var).or_else(|_| env::var(lower_var)) {
            let proxy = proxy_creation(&proxy_str, proxy_type)?;
            return Ok(agent_builder.proxy(proxy).build());
        }
    }

    Ok(agent_builder.build())
}

pub trait ResponseExt {
    fn json<T>(&self) -> Result<T>
    where
        T: DeserializeOwned;

    fn body_string(&self) -> Result<String>;
}

impl ResponseExt for Response<Vec<u8>> {
    fn json<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let body = self.body();
        let result = serde_json::from_slice(body)?;
        Ok(result)
    }

    fn body_string(&self) -> Result<String> {
        let body = self.body();
        let bstr = std::str::from_utf8(body)?;
        Ok(bstr.to_string())
    }
}
