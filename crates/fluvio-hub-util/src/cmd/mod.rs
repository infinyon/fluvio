mod list;
mod download;

pub use list::ConnectorHubListOpts;
pub use download::ConnectorHubDownloadOpts;
use crate::HubAccess;
use crate::PackageListMeta;

use anyhow::Result;
use anyhow::anyhow;

pub fn get_hub_access(remote: &Option<String>) -> Result<HubAccess> {
    HubAccess::default_load(remote)
        .map_err(|_| anyhow!("missing access credentials, try 'fluvio cloud login'"))
}

pub async fn get_pkg_list(endpoint: &str, remote: &Option<String>) -> Result<PackageListMeta> {
    use crate::http;

    let access = get_hub_access(remote)?;

    let action_token = access
        .get_list_token()
        .await
        .map_err(|_| anyhow!("rejected access credentials, try 'fluvio cloud login'"))?;
    let url = format!("{}/{endpoint}", &access.remote);
    let mut res = http::get(&url)
        .header("Authorization", &action_token)
        .await
        .map_err(|e| anyhow!("list api access error {e}"))?;
    res.body_json()
        .await
        .map_err(|e| anyhow!("list api data parse error {e}"))
}
