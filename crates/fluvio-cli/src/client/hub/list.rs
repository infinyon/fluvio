use clap::Parser;

use fluvio_hub_util as hubutil;
use hubutil::PackageList;
use hubutil::http;

use crate::{CliError, Result};

const API_LIST: &str = "hub/v0/list";

/// List available SmartModules in the hub
#[derive(Debug, Parser)]
pub struct ListHubOpt {}

impl ListHubOpt {
    pub async fn process(&self) -> Result<()> {
        let access = hubutil::HubAccess::default_load().await.map_err(|_| {
            CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
        })?;
        let action_token = access.get_list_token().await.map_err(|_| {
            CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
        })?;
        let url = format!("{}/{API_LIST}", &access.remote);
        let mut res = http::get(&url)
            .header("Authorization", &action_token)
            .await
            .map_err(|e| CliError::HubError(format!("list api access error {e}")))?;
        let pl: PackageList = res
            .body_json()
            .await
            .map_err(|e| CliError::HubError(format!("list api data parse error {e}")))?;
        println!("SMARTMODULES");
        for pkg in pl.packages {
            println!("{pkg}");
        }
        Ok(())
    }
}
