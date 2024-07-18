use std::env::var;

use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use semver::Version;

use ureq::OrAnyStatus;

use crate::{
    common::{notify::Notify, update_manager::UpdateManager},
    VERSION,
};

/// Environment variable to store the version of FVM to fetch
const FVM_UPDATE_VERSION: &str = "FVM_UPDATE_VERSION";

/// Default URL used to fetch the `stable` channel tag
const FVM_STABLE_CHANNEL_URL: &str =
    "https://packages.fluvio.io/v1/packages/fluvio/fvm/tags/stable";

#[derive(Clone, Debug, Parser)]
pub struct SelfUpdateOpt;

// https://packages.fluvio.io/v1/packages/fluvio/fvm/0.11.0/aarch64-apple-darwin/fvm
impl SelfUpdateOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let update_manager = UpdateManager::new(&notify);
        let next_version = self.resolve_version().await?;

        if next_version.to_string() != VERSION {
            notify.info(format!(
                "Updating FVM from {} to {}",
                VERSION.red(),
                next_version.to_string().green(),
            ));
            update_manager.update(&next_version).await?;
            return Ok(());
        }

        notify.info("Already up-to-date");
        Ok(())
    }

    /// Determines the version of FVM to fetch taking into account
    /// the environment variable `FVM_VERSION` and the `stable` channel
    async fn resolve_version(&self) -> Result<Version> {
        if let Ok(version) = var(FVM_UPDATE_VERSION) {
            return Ok(Version::parse(&version)?);
        }

        self.fetch_stable_tag().await
    }

    /// Fetches the `stable` channel tag from the Fluvio Version Manager
    async fn fetch_stable_tag(&self) -> Result<Version> {
        // let client = Client::new();
        // let request = client.get(FVM_STABLE_CHANNEL_URL)?;

        let request = ureq::get(FVM_STABLE_CHANNEL_URL);
        let response = request
            .call()
            .or_any_status()
            .map_err(|e| anyhow::anyhow!("Unable to retrieve stable tag for FVM: {e}"))?;

        let version = response.into_string()?;
        let version = Version::parse(&version)?;

        Ok(version)
    }
}
