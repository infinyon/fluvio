use anyhow::Result;
use anyhow::anyhow;
use http;

mod list;
mod download;

pub use list::ConnectorHubListOpts;
pub use download::ConnectorHubDownloadOpts;

use crate::HubAccess;
use crate::PackageListMeta;
use crate::htclient;

pub fn get_hub_access(remote: &Option<String>) -> Result<HubAccess> {
    HubAccess::default_load(remote)
        .map_err(|_| anyhow!("missing access credentials, try 'fluvio cloud login'"))
}

pub async fn get_pkg_list(
    endpoint: &str,
    remote: &Option<String>,
    sysflag: bool,
) -> Result<PackageListMeta> {
    use crate::htclient::ResponseExt;

    let access = get_hub_access(remote)?;
    let action_token = access
        .get_list_token()
        .await
        .map_err(|_| anyhow!("rejected access credentials, try 'fluvio cloud login'"))?;

    let mut uri = format!("{}/{endpoint}", &access.remote);
    if sysflag {
        uri = format!("{uri}?sys=1");
    }

    let req = http::Request::get(&uri)
        .header("Authorization", action_token)
        .body("")
        .map_err(|e| anyhow!("request format error {e}"))?;

    let resp = htclient::send(req)
        .await
        .map_err(|e| anyhow!("list api access error {e}"))?;

    resp.json::<PackageListMeta>().map_err(|e| {
        let body = resp.body_string().unwrap_or_default();
        tracing::debug!(body, "parse err");
        anyhow!("list api data parse error {e}")
    })
}
