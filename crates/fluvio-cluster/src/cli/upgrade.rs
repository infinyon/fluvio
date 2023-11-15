use clap::Parser;
use fluvio_extension_common::installation::InstallationType;
use semver::Version;
use anyhow::{Result, bail};
use tracing::debug;

use crate::{cli::shutdown::ShutdownOpt, cli::get_installation_type};

use super::start::StartOpt;

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    #[clap(flatten)]
    pub start: StartOpt,
}

impl UpgradeOpt {
    pub async fn process(self, platform_version: Version) -> Result<()> {
        let installation_type = get_installation_type()?;
        debug!(?installation_type);
        let requested_installtion_type = self.start.installation_type.get();
        if installation_type != requested_installtion_type {
            bail!("It is not allowed to change installation type during cluster upgrade. Current: {installation_type}, requested: {requested_installtion_type}");
        }
        match installation_type {
            InstallationType::K8 => {
                self.start.process(platform_version, true).await?;
            }
            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                ShutdownOpt.process().await?;
                self.start.process(platform_version, true).await?;
            }
        };

        Ok(())
    }
}
