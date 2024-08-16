use clap::Parser;
use fluvio_extension_common::installation::InstallationType;
use fluvio_types::config_file::SaveLoadConfig;
use semver::Version;
use anyhow::{anyhow, Result, bail};
use tracing::debug;

use crate::{
    cli::{get_installation_type, shutdown::ShutdownOpt},
    start::local::{DEFAULT_RUNNER_PATH, LOCAL_CONFIG_PATH},
    LocalConfig,
};

use super::start::StartOpt;

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    /// Force upgrade without confirmation
    #[clap(short, long)]
    pub force: bool,
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

        if !self.force {
            let prompt = dialoguer::Confirm::new()
                .with_prompt(format!(
                    "Upgrade Local Fluvio cluster to version {}?",
                    platform_version
                ))
                .interact()?;

            if !prompt {
                println!("Upgrade cancelled");
                return Ok(());
            }
        }

        match installation_type {
            InstallationType::K8 => {
                self.start.process(platform_version, true).await?;
            }
            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                ShutdownOpt.process().await?;
                self.upgrade_local_cluster(platform_version)?;
            }
            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("Fluvio cluster upgrade does not operate on cloud cluster \"{profile}\", use 'fluvio cloud ...' commands")
            }
            other => bail!("upgrade command is not supported for {other} installation type"),
        };

        Ok(())
    }

    fn upgrade_local_cluster(&self, platform_version: Version) -> Result<()> {
        let path = LOCAL_CONFIG_PATH
            .as_ref()
            .ok_or(anyhow!("Local config path not set"))?;

        let mut local_config = LocalConfig::load_from(path)
            .map_err(|err| anyhow!("Failed to load local config: {err}"))?
            .evolve();

        let config = local_config
            .platform_version(platform_version.clone())
            .launcher(DEFAULT_RUNNER_PATH.clone())
            .build()?;

        config.save_to(path)?;

        println!(
            "Successfully upgraded Local Fluvio cluster to {}",
            platform_version,
        );
        println!("To start the cluster again, run: fluvio cluster resume");
        Ok(())
    }
}
