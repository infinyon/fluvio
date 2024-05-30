use clap::Parser;
use fluvio_extension_common::installation::InstallationType;
use semver::Version;
use anyhow::{bail, Result};
use tracing::debug;

use crate::cli::{get_installation_type, shutdown::ShutdownOpt};

use super::start::StartOpt;

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    #[clap(flatten)]
    pub start: StartOpt,
}

impl UpgradeOpt {
    pub async fn process(mut self, platform_version: Version) -> Result<()> {
        let (installation_type, config) = get_installation_type()?;
        debug!(?installation_type);
        if let Some(requested) = self.start.installation_type.get() {
            if installation_type != requested {
                bail!("It is not allowed to change installation type during cluster upgrade. Current: {installation_type}, requested: {requested}");
            }
        } else {
            self.start.installation_type.set(installation_type.clone());
        }
        match installation_type {
            InstallationType::K8 => {
                self.start.process(platform_version, true).await?;
            }
            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                ShutdownOpt.process().await?;
                self.start.process(platform_version, true).await?;
            }
            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("Fluvio cluster upgrade does not operate on cloud cluster \"{profile}\", use 'fluvio cloud ...' commands")
            }
            other => bail!("upgrade command is not supported for {other} installation type"),
        };

        Ok(())
    }
}
